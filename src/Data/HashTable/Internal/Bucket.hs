{-# LANGUAGE BangPatterns #-}

module Data.HashTable.Internal.Bucket
  (
    Bucket
  , new
  , size
  , lookup
  , insert
  , delete
  , focus
  , forM
  , mapM
  , forM_
  , mapM_
  , assocs
  ) where

import           Prelude hiding (lookup, mapM, mapM_)
import           Data.IORef
import           Data.Maybe (fromJust)
import qualified Data.Map.Strict as Map
import           Focus (Decision(..), Strategy)
import qualified Focus as Focus
import qualified Control.Monad as Monad

data Bucket k v = Bucket !(IORef (Map.Map k (IORef v)))

new :: IO (Bucket k v)
new = do
  mr <- newIORef Map.empty
  return $ Bucket mr

size :: Bucket k v -> IO Int
size (Bucket mr) = do
  m <- readIORef mr
  return $ Map.size m

lookup :: Ord k => Bucket k v -> k -> IO (Maybe v)
lookup (Bucket mr) !k = do
  m <- readIORef mr
  readValue $ Map.lookup k m

insert :: Ord k => Bucket k v -> k -> v -> IO ()
insert m !k !v = focus m k $ Focus.insert v

delete :: Ord k => Bucket k v -> k -> IO ()
delete (Bucket mr) !k  = modifyIORef' mr $ Map.delete k

focus :: Ord k => Bucket k v -> k -> Strategy v r -> IO r
focus (Bucket mr) !k s = do
  m <- readIORef mr
  retr <- newIORef Nothing
  m' <- Map.alterF (doAlter s retr) k m
  writeIORef mr m'
  fromJust <$> readIORef retr
  where
    doAlter :: Strategy v r -> IORef (Maybe r) -> Maybe (IORef v) -> IO (Maybe (IORef v))
    doAlter strategy retr maybeRef = do
      maybeVal <- readValue maybeRef
      let (!ret, des) = strategy maybeVal
      writeIORef retr (Just ret)
      processDecision maybeRef des

    processDecision :: Maybe (IORef v) -> Decision v -> IO (Maybe (IORef v))
    processDecision vr Keep = return vr
    processDecision _ Remove = return Nothing
    processDecision vr@(Just ref) (Replace !v') = do
      writeIORef ref v'
      return vr
    processDecision Nothing (Replace !v') = do
      vr' <- newIORef v'
      return (Just vr')

forM :: Ord k => Bucket k v -> ((k, v) -> IO a) -> IO [a]
forM (Bucket mr) f = do
  m <- readIORef mr
  Monad.forM (Map.assocs m) (
    \(!k, vr) -> do
        v <- readIORef vr
        f (k, v)
    )

forM_ :: Ord k => Bucket k v -> ((k, v) -> IO a) -> IO ()
forM_ b f = do
  _ <- forM b f
  return ()

mapM :: Ord k => ((k, v) -> IO a) -> Bucket k v -> IO [a]
mapM = flip forM

mapM_ :: Ord k => ((k, v) -> IO a) -> Bucket k v -> IO ()
mapM_ = flip forM_

assocs :: Ord k => Bucket k v  -> IO [(k, v)]
assocs = mapM (return . id)

readValue :: Maybe (IORef v) -> IO (Maybe v)
readValue (Just vr) = do
  v <- readIORef vr
  return (Just v)
readValue Nothing = return Nothing
