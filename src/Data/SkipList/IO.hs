{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BangPatterns #-}

module Data.SkipList.IO
  (
    SkipList
  , new
  , newMaxLevel
  , size
  , lookup
  , insert
  , delete
  , focus
  , adjust
  , assocs
  ) where

import Prelude hiding (lookup)
import Focus (Decision(..), Strategy)
import Data.Atomics
import Data.IORef
import Data.Maybe (fromJust)
import Data.List (uncons)
import Control.Monad.State.Strict
import System.Random (randomIO)

data Node k v = Level {next :: IORef (Node k v), down :: IORef (Node k v)}
              | Head {next :: IORef (Node k v)}
              | Internal {key :: !k, next :: IORef (Node k v), down :: IORef (Node k v), isDeleted :: IORef Bool}
              | Leaf {key :: !k, value :: IORef v, next :: IORef (Node k v), isDeleted :: IORef Bool}
              | Nil
              | Deleted

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

size :: (Ord k) => SkipList k v -> IO Int
size SkipList{headRef} = do
  headTicket <- readForCAS headRef
  evalStateT (countLeafs) [Start headTicket]
  where
    countLeafs :: (Ord k) => StateT (Path k v) IO Int
    countLeafs = do
      _ <- bottom
      doCount

    doCount :: (Ord k) => StateT (Path k v) IO Int
    doCount = do
      node <- stepForward
      case node of
        Nil -> return 0
        Leaf{} -> succ <$> doCount
        _ -> doCount


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
lookup SkipList{headRef} !k = do
  headTicket <- readForCAS headRef
  res <- evalStateT (findLeafByKey k) [Start headTicket]
  case res of
    Just node -> Just <$> readIORef (value node)
    Nothing -> return Nothing

insert :: (Ord k) => SkipList k v -> k -> v -> IO ()
insert SkipList{headRef} !k !v = do
  headTicket <- readForCAS headRef
  (res, path) <- runStateT (findLeafByKey k) [Start headTicket]
  case res of
    Just node -> writeIORef (value node) v
    Nothing -> evalStateT (insertLeaf k v) path

delete :: (Ord k) => SkipList k v -> k -> IO ()
delete SkipList{headRef} !k = do
  headTicket <- readForCAS headRef
  (res, path) <- runStateT (findLeafByKey k) [Start headTicket]
  case res of
    Nothing -> return ()
    Just node -> evalStateT (deleteNode node) path

assocs :: (Ord k) => SkipList k v -> IO [(k, v)]
assocs SkipList{headRef} = do
  headTicket <- readForCAS headRef
  nodes <- evalStateT (bottomLevel) [Start headTicket]
  forM nodes (
    \node -> do
      val <- readIORef (value node)
      return ((key node), val)
    )

adjust :: (Ord k) => SkipList k v -> k -> (v -> (v, r)) -> IO (Maybe r)
adjust SkipList{headRef} !k f = do
  headTicket <- readForCAS headRef
  res  <- evalStateT (findLeafByKey k) [Start headTicket]
  case res of
    Just node -> Just <$> atomicModifyIORefCAS (value node) f
    Nothing -> return Nothing

focus :: (Ord k) => SkipList k v -> k -> Strategy v r -> IO r
focus SkipList{headRef} !k strategy = do
  headTicket <- readForCAS headRef
  (res, path) <- runStateT (findLeafByKey k) [Start headTicket]
  case res of
    Nothing -> do
      let (ret, decision) = strategy Nothing
      case decision of
        Replace value -> do
          evalStateT (insertLeaf k value) path
          return ret
        _ -> return ret
    Just node -> do
      (ret, decision) <- atomicModifyIORefCAS (value node) $
        (\value ->
           let result@(_, decision) = strategy $ Just value
           in
             case decision of
               Replace newValue -> (newValue, result)
               _ -> (value, result)
        )
      case decision of
        Remove -> do
          evalStateT (deleteNode node) path
          return ret
        _ -> return ret

deleteNode :: (Ord k) => Node k v -> StateT (Path k v) IO ()
deleteNode node = do
  levels <- getLevels [node]
  liftIO $ mapM_ markDeleted levels
  doDelete (key node)
    where
      getLevels :: (Ord k) => [Node k v] -> StateT (Path k v) IO [Node k v]
      getLevels nodes  = do
        step <- stepBack
        case step of
          Down{node=newNode} -> getLevels (newNode:nodes)
          _ -> do
            modify (step:)
            return nodes

      doDelete :: (Ord k) => k -> StateT (Path k v) IO ()
      doDelete k = do
        Next{node=prevNode} <- stepBack
        ticket <- liftIO $ readForCAS $ (next prevNode)
        let toDelete = peekTicket ticket
        del <- liftIO $ deleted toDelete
        when (del && k==(key toDelete)) $ do
          nextNode <- liftIO $ readIORef $ next toDelete
          (success, newTicket) <- liftIO $ casIORef (next prevNode) ticket nextNode
          liftIO $ writeIORef (next toDelete) Deleted
          unless (success) $ modify ((Next prevNode newTicket):)
        case toDelete of
          Internal{} -> do
            _ <- backAndDown
            _ <- forwardToGTE k
            doDelete k
          _ -> return ()

findLeafByKey :: (Ord k) => k -> StateT (Path k v) IO (Maybe (Node k v))
findLeafByKey k = do
  item <- findByKey k
  case item of
    Just node -> do
      del <- liftIO $ deleted node
      if del then return Nothing else Just <$> bottom
    Nothing -> return Nothing

findByKey :: (Ord k) => k -> StateT (Path k v) IO (Maybe (Node k v))
findByKey k = do
  levelGTE <- forwardToGTE k
  case checkNode k levelGTE of
    EQ -> return $ Just levelGTE
    GT -> do
      lastStep <- gets head
      case node lastStep of
        Leaf{} -> return Nothing
        Head{} -> return Nothing
        _ -> do
          _ <- backAndDown
          findByKey k
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

bottomLevel :: (Ord k) => StateT (Path k v) IO ([Node k v])
bottomLevel = do
  _ <- bottom
  _ <- stepForward
  nodes <- getLevel
  return [n | n@Leaf{} <- nodes]
  where
    getLevel :: (Ord k) => StateT (Path k v) IO ([Node k v])
    getLevel = do
      node <- stepForward
      case node of
        Nil -> return []
        _ -> (node:) <$> getLevel

bottom :: (Ord k) => StateT (Path k v) IO (Node k v)
bottom = do
  lastStep <- gets head
  downNode <- case lastStep of
    Down{node} -> liftIO $ readIORef (down node)
    _-> liftIO $ return $ peekTicket . nextTicket $ lastStep

  case downNode of
    Internal{} -> do
      t <- liftIO $ readForCAS (next downNode)
      modify ((Down downNode t):)
      bottom
    _ -> return downNode

stepForward :: (Ord k) => StateT (Path k v) IO (Node k v)
stepForward = do
  prevStep <- gets(head)
  nxt <- liftIO $ case prevStep of
    Down{node} -> readIORef $ down node
    _ -> return $ peekTicket (nextTicket prevStep)
  nxtTicket <- liftIO $ nextAlive nxt
  modify ((Next nxt nxtTicket):)
  return (peekTicket nxtTicket)
  where
    nextAlive :: (Ord k) => Node k v -> IO (Ticket (Node k v))
    nextAlive Head{next} = readForCAS next
    nextAlive Level{next} = readForCAS next
    nextAlive node = do
      nextTicket <- readForCAS $ next node
      let nextNode = peekTicket nextTicket
      del <- deleted nextNode
      if del then nextAlive (nextNode) else return nextTicket

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
  newNode <- liftIO $ newLeaf k v (peekTicket nextTicket)
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
      newNode <- liftIO $ newInternal k (peekTicket nextTicket) base
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

newInternal :: (Ord k) => k -> Node k v -> Node k v -> IO (Node k v)
newInternal key next down =
  (Internal key) <$> newIORef next <*> newIORef down <*> newIORef False

newLeaf :: (Ord k) => k -> v -> Node k v -> IO (Node k v)
newLeaf key value next =
  (Leaf key) <$> newIORef value <*> newIORef next <*> newIORef False

markDeleted :: (Ord k) => Node k v -> IO ()
markDeleted node = writeIORef (isDeleted node) (True)

deleted :: (Ord k) => Node k v -> IO Bool
deleted Nil = return False
deleted Head{} = return False
deleted Level{} = return False
deleted node = readIORef $ isDeleted node
