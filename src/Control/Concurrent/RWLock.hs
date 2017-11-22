{-# LANGUAGE NamedFieldPuns #-}

-- ReadWriteLock with writer priority
module Control.Concurrent.RWLock
  (
    RWLock
  ,  new
  , acquireRead
  , acquireWrite
  , releaseRead
  , releaseWrite
  , withRead
  , withWrite
  ) where

import           Data.Typeable
import           Control.Exception (mask_, bracket_)
import           Control.Concurrent.Lock (Lock)
import qualified Control.Concurrent.Lock as Lock
import           Control.Concurrent.MVar

data RWLock = RWLock
  { state :: MVar (State)
  , readLock :: Lock
  , writeLock :: Lock
  } deriving Typeable

data State = Free | Read Int | Write

new :: IO RWLock
new =  RWLock <$> (newMVar Free) <*> Lock.new <*> Lock.new

acquireRead :: RWLock -> IO ()
acquireRead l = mask_ (doAcquire l)
  where
    doAcquire :: RWLock -> IO ()
    doAcquire lock@(RWLock {state, readLock, writeLock}) = do
      Lock.wait writeLock
      st <- takeMVar state
      case st of
        Free -> do
          Lock.acquire readLock
          putMVar state (Read 1)
        Read c -> putMVar state . Read $! succ c
        Write -> do
          putMVar state st
          doAcquire lock

acquireWrite :: RWLock -> IO ()
acquireWrite l = mask_ (doAcquire l)
  where
    doAcquire :: RWLock -> IO ()
    doAcquire lock@(RWLock {state, readLock, writeLock}) = do
      Lock.acquire writeLock
      st <- takeMVar state
      case st of
        Free -> do
          Lock.acquire readLock
          putMVar state Write
        Read _ -> do
          putMVar state st
          waitReaders lock
        Write -> do
          putMVar state st
          error $ moduleName ++ ".acquireWrite: write lock released with Write state"

    waitReaders :: RWLock -> IO ()
    waitReaders lock@(RWLock {state, readLock, writeLock = _}) = do
      Lock.wait readLock
      st <- takeMVar state
      case st of
        Free -> do
          Lock.acquire readLock
          putMVar state Write
        Read _ -> do
          putMVar state st
          waitReaders lock
        Write -> do
          putMVar state st
          error $ moduleName ++ ".acquireWrite: write lock released with Write state"

releaseRead :: RWLock -> IO ()
releaseRead l = mask_ (doRelease l)
  where
    doRelease :: RWLock -> IO ()
    doRelease (RWLock {state, readLock, writeLock = _}) = do
      st <- takeMVar state
      case st of
        Read 1 -> do
          Lock.release readLock
          putMVar state (Free)
        Read c -> putMVar state . Read $! pred c
        _ -> do
          putMVar state st
          error $ moduleName ++ ".releaseRead: already released"

releaseWrite :: RWLock -> IO ()
releaseWrite l = mask_ (doRelease l)
  where
    doRelease :: RWLock -> IO ()
    doRelease (RWLock {state, readLock, writeLock}) = do
      st <- takeMVar state
      case st of
        Write -> do
          Lock.release readLock
          Lock.release writeLock
          putMVar state Free
        _ -> do
          putMVar state st
          error $ moduleName ++ ".releaseWrite: already released"

withRead :: RWLock -> IO a -> IO a
withRead = bracket_ <$> acquireRead <*> releaseRead

withWrite :: RWLock -> IO a -> IO a
withWrite = bracket_ <$> acquireWrite <*> releaseWrite

moduleName :: String
moduleName = "Control.Concurrent.RWLock"
