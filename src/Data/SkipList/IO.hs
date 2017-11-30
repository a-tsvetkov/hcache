{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BangPatterns #-}

module Data.SkipList.IO
  (
    SkipList
  , new
  , newMaxLevel
  , size
  , countItems
  , lookup
  , insert
  , delete
  , focus
  , adjust
  , assocs
  , showNodes
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
              | Internal {key :: !k, next :: IORef (Node k v), down :: IORef (Node k v), isDeleted :: IORef Bool}
              | Head {next :: IORef (Node k v)}
              | Leaf {key :: !k, value :: IORef v, next :: IORef (Node k v), isDeleted :: IORef Bool}
              | Nil deriving Eq

instance (Show k) => Show (Node k v) where
  show Level{} = "Level"
  show Internal{key} = "Internal " ++ show key
  show Head{} = "Head"
  show Leaf{key} = "Leaf " ++ show key
  show Nil = "Nil"

data SkipList k v = SkipList {headRef :: IORef (Node k v), itemCount :: IORef Int}

data PathItem k v = Start {nextTicket :: Ticket (Node k v)}
                  | Next {node :: Node k v, nextTicket :: Ticket (Node k v)}
                  | Down {node :: Node k v, nextTicket :: Ticket (Node k v)}
                  deriving Eq

type Path k v = [PathItem k v]

defaultMaxLevel :: Int
defaultMaxLevel = 16

new :: (Ord k) => IO (SkipList k v)
new = newMaxLevel defaultMaxLevel

size :: (Ord k) => SkipList k v -> IO Int
size SkipList{itemCount} = readIORef itemCount

countItems :: (Ord k) => SkipList k v -> IO Int
countItems SkipList{headRef} = do
  headTicket <- readForCAS headRef
  evalStateT (countLeafs) [Start headTicket]
  where
    countLeafs :: (Ord k) => StateT (Path k v) IO Int
    countLeafs = bottom >> doCount

    doCount :: (Ord k) => StateT (Path k v) IO Int
    doCount = do
      node <- stepForward
      case node of
        Nil -> return 0
        Leaf{} -> do
          del <- deleted node
          if (not del)
            then succ <$> doCount
            else doCount
        _ -> doCount


newMaxLevel :: (Ord k) => Int -> IO (SkipList k v)
newMaxLevel maxLevel = do
  baseLevel <- Head <$> newIORef Nil
  h <- createLevels baseLevel 1
  SkipList <$> (newIORef h) <*> newIORef 0
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
insert SkipList{headRef, itemCount} !k !v = do
  headTicket <- readForCAS headRef
  (res, path) <- runStateT (findLeafByKey k) [Start headTicket]
  case res of
    Just node -> writeIORef (value node) v
    Nothing -> evalStateT (insertLeaf itemCount k v) path

delete :: (Ord k) => SkipList k v -> k -> IO ()
delete SkipList{headRef, itemCount} !k = do
  headTicket <- readForCAS headRef
  (res, path) <- runStateT (findLeafByKey k) [Start headTicket]
  case res of
    Nothing -> return ()
    Just node -> evalStateT (deleteNode itemCount node) path

assocs :: (Ord k) => SkipList k v -> IO [(k, v)]
assocs SkipList{headRef} = do
  headTicket <- readForCAS headRef
  nodes <- evalStateT (bottomLevel) [Start headTicket]
  forM nodes (
    \node -> do
      val <- readIORef (value node)
      return ((key node), val)
    )

  where
    bottomLevel :: (Ord k) => StateT (Path k v) IO ([Node k v])
    bottomLevel = bottom >> reverse <$> getLeafs

    getLeafs :: (Ord k) => StateT (Path k v) IO ([Node k v])
    getLeafs = do
      node <- stepForward
      case node of
        Nil -> return []
        Leaf{} -> do
          del <- deleted node
          if (not del )
            then (node:) <$> getLeafs
            else getLeafs
        _ -> getLeafs

adjust :: (Ord k) => SkipList k v -> k -> (v -> (v, r)) -> IO (Maybe r)
adjust SkipList{headRef} !k f = do
  headTicket <- readForCAS headRef
  res  <- evalStateT (findLeafByKey k) [Start headTicket]
  case res of
    Just node -> Just <$> atomicModifyIORefCAS (value node) f
    Nothing -> return Nothing

focus :: (Ord k) => SkipList k v -> k -> Strategy v r -> IO r
focus SkipList{headRef, itemCount} !k strategy = do
  headTicket <- readForCAS headRef
  (res, path) <- runStateT (findLeafByKey k) [Start headTicket]
  case res of
    Nothing -> do
      let (ret, decision) = strategy Nothing
      case decision of
        Replace value -> do
          evalStateT (insertLeaf itemCount k value) path
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
          evalStateT (deleteNode itemCount node) path
          return ret
        _ -> return ret

showNodes :: (Ord k, Show k) => SkipList k v -> IO ()
showNodes SkipList{headRef} = do
  headTicket <- readForCAS headRef
  nodes <- evalStateT (getAllNodes) [Start headTicket]
  forM_ nodes $ putStrLn . unwords . (map show)
  where
    getAllNodes :: (Ord k) => StateT (Path k v) IO [[Node k v]]
    getAllNodes = do
      start <- gets head
      nodes <- getNodes
      modify (dropWhile (/= start))
      levelHead <- peekNext
      if isBottom levelHead
        then return [nodes]
        else stepForward >> stepDown >> (nodes:) <$> getAllNodes

    getNodes :: (Ord k) => StateT (Path k v) IO ([Node k v])
    getNodes = do
      node <- peekNext
      case node of
        Nil -> return [node]
        _ -> stepForward >> ((node:) <$> getNodes)


deleteNode :: (Ord k) => IORef Int -> Node k v -> StateT (Path k v) IO ()
deleteNode count node = do
  levels <- getLevels [node]
  liftIO $ do
    mapM_ markDeleted levels
    atomicModifyIORefCAS_ count pred
  doDelete (key node)
    where
      getLevels :: (Ord k) => [Node k v] -> StateT (Path k v) IO [Node k v]
      getLevels nodes  = do
        step <- stepBack
        case step of
          Down{node=newNode} -> getLevels (newNode:nodes)
          _ -> modify (step:) >> return nodes

      findDeleted :: (Ord k) => k -> StateT (Path k v) IO (Maybe (Node k v))
      findDeleted k = do
        nextNode <- peekNext
        case checkNode k nextNode of
          GT -> return Nothing
          EQ -> do
            del <- deleted nextNode
            if del
              then return $ Just nextNode
              else stepForward >> findDeleted k
          LT -> stepForward >> findDeleted k

      doDelete :: (Ord k) => k -> StateT (Path k v) IO ()
      doDelete k = do
        toDelete <- findDeleted k
        case toDelete of
          Just n -> do
            Next{node=prevNode, nextTicket=ticket} <- stepBack
            nextNode <- liftIO $ readIORef (next n)
            (success, newTicket) <- liftIO $ casIORef (next prevNode) ticket nextNode
            modify ((Next prevNode newTicket):)
            unless success $ doDelete k
          Nothing -> return ()
        prev <- peekCurrent
        unless (isBottom prev) $ stepDown >> doDelete k

findLeafByKey :: (Ord k) => k -> StateT (Path k v) IO (Maybe (Node k v))
findLeafByKey k = do
  item <- findByKey k
  case item of
    Just node -> do
      del <- deleted node
      if del then return Nothing else Just <$> bottom
    Nothing -> return Nothing

findByKey :: (Ord k) => k -> StateT (Path k v) IO (Maybe (Node k v))
findByKey k = do
  levelGTE <- forwardToGTE k
  case checkNode k levelGTE of
    EQ -> return $ Just levelGTE
    GT -> do
      currentNode <- peekCurrent
      if  isBottom currentNode
        then return Nothing
        else stepDown >> findByKey k
    LT -> error "levelGTE returned LT node"

stepDown :: StateT (Path k v) IO ()
stepDown = do
  step <- stepBack
  case step of
    Next{node, nextTicket} -> modify ((Down node nextTicket):)
    Down{node} -> do
      modify (step:)
      downNode <- liftIO $ readIORef (down node)
      ticket <- liftIO $ readForCAS (next downNode)
      modify ((Down downNode ticket):)
    _ -> error "Tried to go back on a start"


stepBack :: StateT (Path k v) IO (PathItem k v)
stepBack = state (fromJust . uncons)

bottom :: (Ord k) => StateT (Path k v) IO (Node k v)
bottom = do
  lastStep <- gets head
  downNode <- case lastStep of
    Down{node} -> liftIO $ readIORef (down node)
    _-> liftIO $ return $ peekTicket . nextTicket $ lastStep

  if isBottom downNode
    then return downNode
    else
    do
      t <- liftIO $ readForCAS (next downNode)
      modify ((Down downNode t):)
      bottom

stepForward :: (Ord k) => StateT (Path k v) IO (Node k v)
stepForward = do
  prevStep <- gets head
  nxt <- liftIO $ case prevStep of
    Down{node} -> readIORef $ down node
    _ -> return $ peekTicket (nextTicket prevStep)
  nxtTicket <- liftIO $ readForCAS (next nxt)
  modify ((Next nxt nxtTicket):)
  return (peekTicket nxtTicket)

nextAlive :: (Ord k) => Node k v -> StateT (Path k v) IO (Ticket (Node k v))
nextAlive Head{next} = liftIO $ readForCAS next
nextAlive Level{next} = liftIO $ readForCAS next
nextAlive node = do
  nextTicket <- liftIO $ readForCAS $ next node
  let nextNode = peekTicket nextTicket
  del <- deleted nextNode
  if del then nextAlive (nextNode) else return nextTicket

peekNext :: (Ord k) => StateT (Path k v) IO (Node k v)
peekNext = do
  step <- gets (head)
  case step of
    Start{nextTicket} -> return $ peekTicket nextTicket
    Next{nextTicket} -> return $ peekTicket nextTicket
    Down{node} -> liftIO $ readIORef (down node)

peekCurrent :: (Ord k) => StateT (Path k v) IO (Node k v)
peekCurrent = gets (node . head)

backToAlive :: (Ord k) => StateT (Path k v) IO ()
backToAlive = do
  node <- peekCurrent
  del <- deleted node
  if del
    then stepBack >> backToAlive
    else return ()

forwardToGTE :: (Ord k) => k -> StateT (Path k v) IO (Node k v)
forwardToGTE k = do
  node <- peekNext
  case checkNode k node of
    LT -> do
      nextNode <- stepForward
      del <- deleted nextNode
      when del $ do
        Next{} <- stepBack
        alive <- nextAlive node
        modify (Next{node, nextTicket=alive}:)
      forwardToGTE k
    _ -> return node

checkNode :: (Ord k) => k -> Node k v -> Ordering
checkNode _ Nil = GT
checkNode _ Head{} = LT
checkNode _ Level{} = LT
checkNode k node = compare k (key node)

insertLeaf :: (Ord k) => IORef Int -> k -> v -> StateT (Path k v) IO ()
insertLeaf count k v = do
  Next{node, nextTicket} <- stepBack
  del <- deleted node
  if del
    then backToAlive >> findByKey k >> insertLeaf count k v
    else
    do
      newNode <- liftIO $ newLeaf k v (peekTicket nextTicket)
      res <- liftIO $ casIORef (next node) nextTicket newNode
      case res of
        (True, _) -> do
          liftIO $ atomicModifyIORefCAS_ count (succ)
          promote <- liftIO (randomIO :: IO Bool)
          when promote $ insertLevels newNode
        (False, newTicket) -> do
          modify ((Next node newTicket):)
          newBefore <- forwardToGTE k
          case checkNode k newBefore of
            GT -> insertLeaf count k v
            EQ -> liftIO $ writeIORef (value newBefore) v
            LT -> error "forwardToGTE returned LT node"

insertLevels :: (Ord k) => Node k v -> StateT (Path k v) IO ()
insertLevels base = do
  let k = key base
  step <- stepBack
  case step of
    Next{} -> insertLevels base
    Down{node, nextTicket} -> do
      del <- deleted node
      if del
        then backToAlive >> findByKey k >> insertLevels base
        else
        do
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

isBottom :: (Ord k) => Node k v -> Bool
isBottom Leaf{} = True
isBottom Head{} = True
isBottom _ = False

newLeaf :: (Ord k) => k -> v -> Node k v -> IO (Node k v)
newLeaf key value next =
  (Leaf key) <$> newIORef value <*> newIORef next <*> newIORef False

markDeleted :: (Ord k) => Node k v -> IO ()
markDeleted node = writeIORef (isDeleted node) (True)

deleted :: (Ord k) => Node k v -> StateT (Path k v) IO Bool
deleted Nil = return False
deleted Head{} = return False
deleted Level{} = return False
deleted node = liftIO $ readIORef $ isDeleted node
