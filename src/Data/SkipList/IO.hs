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

import           Prelude hiding (lookup, traverse)
import           Focus (Decision(..), Strategy)
import           Data.IORef.Marked
import           Data.List (uncons)
import           Data.Vector (Vector, (!))
import qualified Data.Vector as Vector
import           Control.Monad.State.Strict
import           System.Random (randomIO)

data Node k v = Head {next :: MarkedIORef (Node k v), level :: Int}
              | Internal {key :: !k, next :: MarkedIORef (Node k v), down :: MarkedIORef (Node k v)}
              | Leaf {key :: !k, value :: MarkedIORef v, next :: MarkedIORef (Node k v)}
              | End
              deriving Eq

instance (Show k) => Show (Node k v) where
  show Internal{key} = "Internal " ++ show key
  show Head{} = "Head"
  show Leaf{key} = "Leaf " ++ show key
  show End = "End"

data SkipList k v = SkipList { startRef :: MarkedIORef (Int)
                             , levels :: Vector (Node k v)
                             , itemCount :: MarkedIORef Int
                             }

data PathItem k v = Next {node :: Node k v, nextTicket :: Ticket (Node k v)}
                  | Down {node :: Node k v, nextTicket :: Ticket (Node k v)}
                  deriving Eq

data Path k v = Path { items :: [PathItem k v]
                     , sl :: SkipList k v
                     , startLevel :: Int
                     }

defaultMaxLevel :: Int
defaultMaxLevel = 16

new :: (Ord k) => IO (SkipList k v)
new = newMaxLevel defaultMaxLevel

newMaxLevel :: (Ord k) => Int -> IO (SkipList k v)
newMaxLevel maxLevel = do
  SkipList <$>
    newIORef 0 <*>
    Vector.generateM maxLevel createLevel <*>
    newIORef 0
  where
    createLevel :: (Ord k) => Int -> IO (Node k v)
    createLevel l = Head <$> newIORef End <*> return l

size :: (Ord k) => SkipList k v -> IO Int
size SkipList{itemCount} = readIORef itemCount

enterLevel :: (Ord k) => SkipList k v -> Int -> IO [PathItem k v]
enterLevel SkipList{levels} ln = do
  let startLevel = levels ! ln
  ticket <- nextAlive startLevel
  return [Next startLevel ticket]

traverse :: (Ord k) => SkipList k v -> StateT (Path k v) IO r -> IO r
traverse sl@SkipList{startRef} func  = do
  startLev <- readIORef startRef
  start <- enterLevel sl startLev
  evalStateT (func) $ Path start sl startLev

traverseLevel :: (Ord k) => SkipList k v -> Int -> StateT (Path k v) IO r -> IO r
traverseLevel sl ln func  = do
  start <- enterLevel sl ln
  evalStateT (func) $ Path start sl ln

countItems :: (Ord k) => SkipList k v -> IO Int
countItems sl = do
  traverseLevel sl 0 countLeafs
  where
    countLeafs :: (Ord k) => StateT (Path k v) IO Int
    countLeafs = do
      node <- peekNext
      case node of
        End -> return 0
        Leaf{} -> do
          stepForward
          del <- liftIO $ deleted node
          if (not del)
            then succ <$> countLeafs
            else countLeafs
        _ -> error "Only leafs must be at level 0"

lookup :: (Ord k) => SkipList k v -> k -> IO (Maybe v)
lookup sl !k = do
  res <- traverse sl (findLeafByKey k)
  case res of
    Just node -> Just <$> readIORef (value node)
    Nothing -> return Nothing

insert :: (Ord k) => SkipList k v -> k -> v -> IO ()
insert sl!k !v = traverse sl (doInsert k v)
  where
    doInsert :: (Ord k) => k -> v -> StateT (Path k v) IO ()
    doInsert !k' !v' = do
      res <- findLeafByKey k'
      case res of
        Just node -> liftIO $ writeIORef (value node) v'
        Nothing -> insertLeaf k' v'

delete :: (Ord k) => SkipList k v -> k -> IO ()
delete sl !k = traverse sl (doDelete k)
  where
    doDelete :: (Ord k) => k -> StateT (Path k v) IO ()
    doDelete !k' = do
      res  <- findLeafByKey k'
      case res of
        Nothing -> return ()
        Just node -> deleteNode node

assocs :: (Ord k) => SkipList k v -> IO [(k, v)]
assocs sl = do
  nodes <- traverseLevel sl 0 getLeafs
  forM nodes (
    \node -> do
      val <- readIORef (value node)
      return ((key node), val)
    )
  where
    getLeafs :: (Ord k) => StateT (Path k v) IO ([Node k v])
    getLeafs = do
      node <- peekNext
      case node of
        End -> return []
        Leaf{} -> do
          stepForward
          del <- liftIO $ deleted node
          if (not del )
            then (node:) <$> getLeafs
            else getLeafs
        _ -> error "Only the leafs must be at the bottom level"

adjust :: (Ord k) => SkipList k v -> k -> (v -> (v, r)) -> IO (Maybe r)
adjust sl !k f = traverse sl (doAdjust k f)
  where
    doAdjust :: (Ord k) => k -> (v -> (v, r)) -> StateT (Path k v) IO (Maybe r)
    doAdjust !k' f' = do
      res  <- findLeafByKey k'
      case res of
        Just node -> liftIO $ Just <$> atomicModifyIORefCAS (value node) f'
        Nothing -> return Nothing

focus :: (Ord k) => SkipList k v -> k -> Strategy v r -> IO r
focus sl !k s = traverse sl (doFocus k s)
  where
    doFocus :: (Ord k) => k -> Strategy v r -> StateT (Path k v) IO r
    doFocus !k' strategy = do
      res <- findLeafByKey k'
      case res of
        Nothing -> do
          let (ret, decision) = strategy Nothing
          case decision of
            Replace value -> do
              insertLeaf k' value
              return ret
            _ -> return ret
        Just node -> do
          (ret, decision) <- liftIO$ atomicModifyIORefCAS (value node) $
            (\value ->
               let result@(_, decision) = strategy $ Just value
               in
                 case decision of
                   Replace newValue -> (newValue, result)
                   _ -> (value, result)
            )
          case decision of
            Remove -> do
              deleteNode node
              return ret
            _ -> return ret

showNodes :: (Ord k, Show k) => SkipList k v -> IO ()
showNodes sl = do
  nodes <- traverse sl (getAllNodes)
  forM_ nodes $ putStrLn . unwords . (map show)
  where
    getAllNodes :: (Ord k) => StateT (Path k v) IO [[Node k v]]
    getAllNodes = do
      start <- lastStep
      nodes <- getNodes
      backTo start
      nextNode <- peekNext
      if isBottom nextNode
        then return [nodes]
        else stepDown >> (nodes:) <$> getAllNodes

    getNodes :: (Ord k) => StateT (Path k v) IO ([Node k v])
    getNodes = do
      node <- peekNext
      case node of
        End -> return [node]
        _ -> stepForward >> ((node:) <$> getNodes)


deleteNode :: (Ord k) => Node k v -> StateT (Path k v) IO ()
deleteNode node = do
  levels <- getLevels [node]
  count <- gets (itemCount . sl)
  liftIO $ do
    mapM_ markDeleted levels
    atomicModifyIORefCAS_ count pred
  doDelete (key node)
    where
      getLevels :: (Ord k) => [Node k v] -> StateT (Path k v) IO [Node k v]
      getLevels nodes  = do
        s <- stepBack
        case s of
          Down{node=newNode} -> getLevels (newNode:nodes)
          _ -> step s >> return nodes

      stepForwardDeleted :: (Ord k) => StateT (Path k v) IO ()
      stepForwardDeleted = do
        nxt <- peekNext
        nxtTicket <- liftIO $ readForCAS (next nxt)
        step (Next nxt nxtTicket)

      findDeleted :: (Ord k) => k -> StateT (Path k v) IO (Maybe (Node k v))
      findDeleted k = do
        nextNode <- peekNext
        case checkNode k nextNode of
          LT -> return Nothing
          EQ -> do
            del <- liftIO $ deleted nextNode
            if del
              then return $ Just nextNode
              else stepForwardDeleted >> findDeleted k
          GT -> stepForwardDeleted >> findDeleted k

      doDelete :: (Ord k) => k -> StateT (Path k v) IO ()
      doDelete k = do
        toDelete <- findDeleted k
        case toDelete of
          Just n -> do
            Next{node=prevNode, nextTicket=ticket} <- stepBack
            nextNode <- liftIO $ readIORef (next n)
            (success, newTicket) <- liftIO $ casIORef (next prevNode) ticket nextNode
            step (Next prevNode newTicket)
            unless success $ doDelete k
          Nothing -> return ()
        prev <- peekCurrent
        unless (isBottom prev) $ stepDown >> doDelete k

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
    LT -> do
      currentNode <- peekCurrent
      if isBottom currentNode
        then return Nothing
        else stepDown >> findByKey k
    GT -> error "levelGTE returned LT node"

setPath :: (Ord k) => [PathItem k v] -> StateT (Path k v) IO ()
setPath newItems = modify (\Path{sl, startLevel} -> Path newItems sl startLevel)

step :: (Ord k) => PathItem k v -> StateT (Path k v) IO ()
step item = gets ((item:) . items) >>= setPath

backTo :: (Ord k) => PathItem k v -> StateT (Path k v) IO ()
backTo item = gets (dropWhile (/=item) . items) >>= setPath

lastStep :: (Ord k) => StateT (Path k v) IO (PathItem k v)
lastStep = do
  s <- get
  case s of
    Path{items=(a:_)} -> return a
    Path{items=[]} -> error "lastStep with empty path"

stepDown :: (Ord k) => StateT (Path k v) IO ()
stepDown = do
  ms <- stepBack
  case ms of
    Next{node, nextTicket} -> step (Down node nextTicket)
    Down{} -> do
      step ms
      downNode <- peekNext
      ticket <- liftIO $ readForCAS (next downNode)
      step (Down downNode ticket)

stepBack :: StateT (Path k v) IO (PathItem k v)
stepBack = do
  maybeStep <- stepBackSafe
  case maybeStep of
    Just st -> return st
    Nothing -> error "Empty path on stepBack"

stepBackSafe :: StateT (Path k v) IO (Maybe (PathItem k v))
stepBackSafe = do
  Path{items, sl, startLevel} <- get
  case uncons items of
    Just (st, rest) -> do
      put (Path rest sl startLevel)
      return $ Just st
    Nothing -> return Nothing

bottom :: (Ord k) => StateT (Path k v) IO (Node k v)
bottom = do
  ls <- lastStep
  downNode <- case ls of
    Down{node} -> liftIO $ readIORef (down node)
    Next{nextTicket}-> liftIO $ return $ peekTicket  nextTicket

  if isBottom downNode
    then return downNode
    else
    do
      t <- liftIO $ readForCAS (next downNode)
      step (Down downNode t)
      bottom

stepForward :: (Ord k) => StateT (Path k v) IO ()
stepForward = do
  nxt <- peekNext
  nxtTicket <- liftIO $ nextAlive nxt
  step (Next nxt nxtTicket)

nextAlive :: (Ord k) => Node k v -> IO (Ticket (Node k v))
nextAlive node = do
  nextTicket <- liftIO $ readForCAS $ next node
  let nextNode = peekTicket nextTicket
  del <- deleted nextNode
  if del then nextAlive (nextNode) else return nextTicket

peekNext :: (Ord k) => StateT (Path k v) IO (Node k v)
peekNext = do
  st <- lastStep
  case st of
    Next{nextTicket} -> return $ peekTicket nextTicket
    Down{node} ->
      case node of
        Head{level} -> gets ((! (pred level)) . levels . sl)
        Internal{} -> liftIO $ readIORef (down node)
        _ -> error "tried to go down on non-internal node"

peekCurrent :: (Ord k) => StateT (Path k v) IO (Node k v)
peekCurrent = node <$> lastStep

backToAlive :: (Ord k) => StateT (Path k v) IO ()
backToAlive = do
  node <- peekCurrent
  del <- liftIO $ deleted node
  if del
    then stepBack >> backToAlive
    else return ()

forwardToGTE :: (Ord k) => k -> StateT (Path k v) IO (Node k v)
forwardToGTE k = do
  node <- peekNext
  case checkNode k node of
    GT -> do
      stepForward >> forwardToGTE k
    _ -> return node

checkNode :: (Ord k) => k -> Node k v -> Ordering
checkNode _ End = LT
checkNode _ Head{} = GT
checkNode k node = compare k (key node)

insertLeaf :: (Ord k) => k -> v -> StateT (Path k v) IO ()
insertLeaf k v = do
  Next{node, nextTicket} <- stepBack
  del <- liftIO $ deleted node
  if del
    then backToAlive >> findByKey k >> insertLeaf k v
    else
    do
      newNode <- liftIO $ newLeaf k v (peekTicket nextTicket)
      res <- liftIO $ casIORef (next node) nextTicket newNode
      case res of
        (True, _) -> do
          count <- gets (itemCount . sl)
          liftIO $ atomicModifyIORefCAS_ count (succ)
          promote <- liftIO (randomIO :: IO Bool)
          when promote $ insertLevels newNode
        (False, newTicket) -> do
          step (Next node newTicket)
          newBefore <- forwardToGTE k
          case checkNode k newBefore of
            LT -> insertLeaf k v
            EQ -> liftIO $ writeIORef (value newBefore) v
            GT -> error "forwardToGTE returned LT node"

insertLevels :: (Ord k) => Node k v -> StateT (Path k v) IO ()
insertLevels base = do
  let k = key base
  s <- stepBackSafe
  case s of
    Nothing -> do
      Path{sl=l@SkipList{levels, startRef}, startLevel} <- get
      let levNum = succ startLevel
      if levNum < (length levels)
        then
        do
          liftIO $ atomicModifyIORefCAS_ startRef (max levNum)
          start <- liftIO $ enterLevel l levNum
          put(Path start l levNum)
          Next{node, nextTicket} <- forwardToGTE k >> lastStep
          insertLevel base node nextTicket
        else return ()
    Just Next{} -> insertLevels base
    Just Down{node, nextTicket} -> insertLevel base node nextTicket
  where
    insertLevel :: (Ord k) => Node k v -> Node k v -> Ticket (Node k v) -> StateT (Path k v) IO ()
    insertLevel base' before after = do
      let k = key base'
      del <- liftIO $ deleted before
      if del
        then backToAlive >> findByKey k >> insertLevels base'
        else
        do
          newNode <- liftIO $ newInternal k (peekTicket after) base'
          res <- liftIO $ casIORef (next before) after newNode
          case res of
            (True, _) -> do
              promote <- liftIO (randomIO :: IO Bool)
              when promote $ insertLevels newNode
            (False, newTicket) -> do
              step (Next before newTicket)
              lastSuccessfull <- findByKey k
              case lastSuccessfull of
                Just n -> insertLevels n
                Nothing -> return ()

isBottom :: (Ord k) => Node k v -> Bool
isBottom Leaf{} = True
isBottom Head{level=0} = True
isBottom Head{} = False
isBottom _ = False

newInternal :: (Ord k) => k -> Node k v -> Node k v -> IO (Node k v)
newInternal key next down = (Internal key) <$> newIORef next <*> newIORef down

newLeaf :: (Ord k) => k -> v -> Node k v -> IO (Node k v)
newLeaf key value next = (Leaf key) <$> newIORef value <*> newIORef next

markDeleted :: (Ord k) => Node k v -> IO ()
markDeleted node = markIORef (next node)

deleted :: (Ord k) => Node k v -> IO Bool
deleted End = return False
deleted Head{} = return False
deleted node = liftIO $ isMarked $ next node
