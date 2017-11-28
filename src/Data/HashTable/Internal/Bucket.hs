{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NamedFieldPuns #-}

module Data.HashTable.Internal.Bucket
  (
    Bucket
  , new
  , size
  , lookup
  , insert
  , delete
  , adjust
  , focus
  , forM
  , mapM
  , forM_
  , mapM_
  , assocs
  ) where

import           Prelude hiding (lookup, mapM, mapM_)
import           Data.Atomics
import           Data.IORef
import           Data.Maybe (isJust, isNothing)
import           Data.SkipList.IO (SkipList)
import qualified Data.SkipList.IO as SkipList
import           Focus (Decision(..), Strategy)
import qualified Control.Monad as Monad

data Bucket k v = Bucket {items :: SkipList k v, numItems :: IORef Int}

skipListLevels :: Int
skipListLevels = 5

new :: (Ord k) => IO (Bucket k v)
new = Bucket <$> (SkipList.newMaxLevel skipListLevels) <*> newIORef 0

size :: Bucket k v -> IO Int
size Bucket{numItems} = readIORef numItems

lookup :: (Ord k) => Bucket k v -> k -> IO (Maybe v)
lookup Bucket{items} !k = SkipList.lookup items k

insert :: Ord k => Bucket k v -> k -> v -> IO ()
insert Bucket{items, numItems} !k !v = do
  SkipList.insert items k v
  atomicModifyIORefCAS_ numItems (succ)

delete :: Ord k => Bucket k v -> k -> IO ()
delete Bucket{items, numItems} !k = do
  SkipList.delete items k
  atomicModifyIORefCAS_ numItems (pred)

adjust :: (Ord k) => Bucket k v -> k -> (v -> (v, r)) -> IO (Maybe r)
adjust Bucket{items} !k f = SkipList.adjust items k f

focus :: (Ord k) => Bucket k v -> k -> Strategy v r -> IO r
focus Bucket{items, numItems} !k strategy = do
  (value, res, decision) <- SkipList.focus items k (
    \value ->
      let (ret, decision) = strategy value
      in ((value, ret, decision), decision)
    )
  case decision of
    Replace _ -> Monad.when (isNothing value) $ atomicModifyIORefCAS_ numItems (succ)
    Remove -> Monad.when (isJust value) $ atomicModifyIORefCAS_ numItems (pred)
    _ -> return ()
  return res


forM :: Ord k => Bucket k v -> ((k, v) -> IO a) -> IO [a]
forM Bucket{items} f = do
  as <- SkipList.assocs items
  Monad.forM as f

forM_ :: Ord k => Bucket k v -> ((k, v) -> IO a) -> IO ()
forM_ b f = do
  _ <- forM b f
  return ()

mapM :: Ord k => ((k, v) -> IO a) -> Bucket k v -> IO [a]
mapM = flip forM

mapM_ :: Ord k => ((k, v) -> IO a) -> Bucket k v -> IO ()
mapM_ = flip forM_

assocs :: Ord k => Bucket k v  -> IO [(k, v)]
assocs Bucket{items} = SkipList.assocs items
