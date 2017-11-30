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
import           Focus (Strategy)
import qualified Control.Monad as Monad

data Bucket k v = Bucket {items :: SkipList k v}

skipListLevels :: Int
skipListLevels = 5

new :: (Ord k) => IO (Bucket k v)
new = Bucket <$> (SkipList.newMaxLevel skipListLevels)

size :: (Ord k) => Bucket k v -> IO Int
size Bucket{items} = SkipList.size items

lookup :: (Ord k) => Bucket k v -> k -> IO (Maybe v)
lookup Bucket{items} !k = SkipList.lookup items k

insert :: Ord k => Bucket k v -> k -> v -> IO ()
insert Bucket{items} !k !v = SkipList.insert items k v

delete :: Ord k => Bucket k v -> k -> IO ()
delete Bucket{items} !k = SkipList.delete items k

adjust :: (Ord k) => Bucket k v -> k -> (v -> (v, r)) -> IO (Maybe r)
adjust Bucket{items} !k f = SkipList.adjust items k f

focus :: (Ord k) => Bucket k v -> k -> Strategy v r -> IO r
focus Bucket{items} !k strategy = do SkipList.focus items k strategy

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
