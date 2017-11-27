{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BangPatterns #-}

module Data.SkipList.IO
  (
    SkipList
  , new
  , lookup
  , insert
  ) where

import Prelude hiding (lookup)
import Data.Atomics
import Data.IORef
import Data.Maybe (fromJust)
import Data.List (uncons)
import Control.Monad.State.Strict
import System.Random (randomIO)

data Node k v = Level {next :: IORef (Node k v), down :: IORef (Node k v)}
              | Head {next :: IORef (Node k v)}
              | Internal {key :: !k, next :: IORef (Node k v), down :: IORef (Node k v)}
              | Leaf {key :: !k, value :: IORef v, next :: IORef (Node k v)}
              | Nil

data SkipList k v = SkipList {headRef :: IORef (Node k v)}

data PathItem k v = Start {nextTicket :: Ticket (Node k v)}
                  | Next {node :: Node k v, nextTicket :: Ticket (Node k v)}
                  | Down {node :: Node k v, nextTicket :: Ticket (Node k v)}

type Path k v = [PathItem k v]

defaultMaxLevel :: Int
defaultMaxLevel = 16

new :: (Ord k) => IO (SkipList k v)
new = do
  newMaxLevel defaultMaxLevel

newMaxLevel :: (Ord k) => Int -> IO (SkipList k v)
newMaxLevel maxLevel = do
  baseLevel <- Head <$> newIORef Nil
  h <- createLevels baseLevel 1
  SkipList <$> (newIORef h)
  where
    createLevels :: (Ord k) => Node k v -> Int -> IO (Node k v)
    createLevels base level
      | level == maxLevel = return base
      | otherwise = do
          l <- Level <$> newIORef Nil <*> newIORef base
          createLevels l (succ level)

lookup :: (Ord k) => SkipList k v -> k -> IO (Maybe v)
lookup SkipList{headRef} k = do
  headTicket <- readForCAS headRef
  node <- evalStateT (findGTE k) [Start headTicket]
  case checkNode k node of
    EQ -> Just <$> readIORef (value node)
    GT -> return Nothing
    LT -> error "findGTE returned LT node"

findGTE :: (Ord k) => k -> StateT (Path k v) IO (Node k v)
findGTE k = do
  levelGTE <- forwardToGTE k
  lastStep <- gets head
  case node lastStep of
    Leaf{} -> return levelGTE
    Head{} -> return levelGTE
    _ ->
      case checkNode k levelGTE of
        EQ -> do
          t <- liftIO $ readForCAS (next levelGTE)
          modify((Down levelGTE t):)
          bottom
        GT -> do
          _ <- backAndDown
          findGTE k
        LT -> error "levelGTE returned LT node"

backAndDown :: StateT (Path k v) IO (Node k v)
backAndDown = do
  step <- stepBack
  case step of
    Next{node, nextTicket} -> do
      modify ((Down node nextTicket):)
      liftIO $ readIORef (down node)
    Down{node} -> do
      modify (step:)
      downNode <- liftIO $ readIORef (down node)
      ticket <- liftIO $ readForCAS (next downNode)
      modify ((Down downNode ticket):)
      liftIO $ readIORef (down downNode)
    _ -> error "Tried to go back on a start"


stepBack :: StateT (Path k v) IO (PathItem k v)
stepBack = state (fromJust . uncons)

bottom :: (Ord k) => StateT (Path k v) IO (Node k v)
bottom = do
  downRef <- gets (down . node . head)
  downNode <- liftIO $ readIORef downRef
  case downNode of
    Internal{} -> do
      t <- liftIO $ readForCAS (next downNode)
      modify ((Down downNode t):)
      bottom
    _ -> return downNode

stepForward :: StateT (Path k v) IO (Node k v)
stepForward = do
  prevStep <- gets(head)
  nxt <- liftIO $ case prevStep of
    Down{node} -> readIORef $ down node
    _ -> return $ peekTicket (nextTicket prevStep)
  nxtTicket <- liftIO $ readForCAS (next nxt)
  modify ((Next nxt nxtTicket):)
  return (peekTicket nxtTicket)

forwardToGTE :: (Ord k) => k -> StateT (Path k v) IO (Node k v)
forwardToGTE k = do
  nxt <- stepForward
  case checkNode k nxt of
    LT -> forwardToGTE k
    _ -> return nxt

checkNode :: (Ord k) => k -> Node k v -> Ordering
checkNode _ Nil = GT
checkNode _ Head{} = LT
checkNode _ Level{} = LT
checkNode k node = compare k (key node)

insertLeaf :: (Ord k) => k -> v -> StateT (Path k v) IO ()
insertLeaf k v = do
  Next{node, nextTicket} <- stepBack
  newNode <- liftIO $ (Leaf k) <$> newIORef v <*> newIORef (peekTicket nextTicket)
  res <- liftIO $ casIORef (next node) nextTicket newNode
  case res of
    (True, _) -> do
      promote <- liftIO (randomIO :: IO Bool)
      when promote $ insertLevels newNode
    (False, newTicket) -> do
      modify ((Next node newTicket):)
      newBefore <- forwardToGTE k
      case checkNode k newBefore of
        GT -> insertLeaf k v
        EQ -> liftIO $ writeIORef (value newBefore) v
        LT -> error "forwardToGTE returned LT node"

insertLevels :: (Ord k) => Node k v -> StateT (Path k v) IO ()
insertLevels base = do
  let k = key base
  step <- stepBack
  case step of
    Next{} -> insertLevels base
    Down{node, nextTicket} -> do
      newNode <- liftIO $ (Internal k) <$> newIORef (peekTicket nextTicket) <*> newIORef base
      res <- liftIO $ casIORef (next node) nextTicket newNode
      case res of
        (True, _) -> do
          promote <- liftIO (randomIO :: IO Bool)
          when promote $ insertLevels newNode
        (False, newTicket) -> do
          modify ((Next node newTicket):)
          newBefore <- forwardToGTE k
          case checkNode k newBefore of
            GT -> insertLevels base
            EQ -> return ()
            LT -> error "forwardToGTE returrned LT node"
    Start{} -> return ()

insert :: (Ord k) => SkipList k v -> k -> v -> IO ()
insert SkipList {headRef} k v = do
  headTicket <- readForCAS headRef
  (node, path) <- runStateT (findGTE k) [Start headTicket]
  case checkNode k node of
    GT -> evalStateT (insertLeaf k v) path
    EQ -> writeIORef (value node) v
    LT -> error "findGTE returned LT node"
