{-# LANGUAGE NamedFieldPuns #-}

module Data.HashTable.IO.ConcurrentLinear
  (
    HashTable
  , new
  , newSized
  , lookup
  , insert
  , delete
  , focus
  ) where

import           Prelude hiding (lookup)
import           Data.Bits
import           Data.Vector.Mutable (IOVector)
import qualified Data.Vector.Mutable as Vector
import           Data.Hashable
import           Data.HashTable.Internal.Bucket (Bucket)
import qualified Data.HashTable.Internal.Bucket as Bucket
import           Data.HashTable.Internal.Utils
import           Data.IORef
import           Control.Concurrent.RWLock (RWLock)
import qualified Control.Concurrent.RWLock as Lock
import           Control.Monad
import           Focus (Strategy)

data HashTable k v = HT
  { _globalLock :: RWLock
  , _htRef :: IORef (HashTable_ k v)
  }

data HashTable_ k v = HashTable
  { _level :: {-# UNPACK #-} !Int
  , _splitPtr :: {-# UNPACK #-} !Int
  , _buckets :: {-# UNPACK #-} !(IOVector (Bucket k v))
  , _locks :: {-# UNPACK #-} !(IOVector RWLock)
  }

bucketSplitSize :: Int
bucketSplitSize = 16

fillFactor :: Double
fillFactor = 1.3

createBucketArray :: Int -> IO (IOVector (Bucket k v))
createBucketArray size = Vector.replicateM size Bucket.new

createLockArray :: Int -> IO (IOVector RWLock)
createLockArray size = Vector.replicateM size Lock.new

newLeveled :: Int -> IO (HashTable k v)
newLeveled level = do
  let size = 2^level
  ht <- HashTable level 0 <$> createBucketArray size <*> createLockArray size
  HT <$> Lock.new <*> newIORef ht

new :: IO (HashTable k v)
new = newLeveled 1

newSized :: Int -> IO (HashTable k v)
newSized size = do
  let k = fromIntegral $ ceiling (fromIntegral size * fillFactor / fromIntegral bucketSplitSize)
      level = max 1 (fromEnum $ log2 k)
  newLeveled level

hashKey :: (Hashable k) => (Ord k, Hashable k) => (HashTable_ k v) -> k -> Int
hashKey (HashTable lvl splitPtr _ _) k =
  let h = hashAtLvl (lvl-1) k
  in
    if (h < splitPtr) then hashAtLvl lvl k else h
  where
    hashAtLvl :: (Hashable k) => Int -> k -> Int
    hashAtLvl l k' =
      let hashcode = hash k'
          mask = 2^l - 1 :: Int
      in
        hashcode .&. mask

getLockAndBucket :: (Ord k, Hashable k) => (HashTable_ k v) -> k -> IO (Bucket k v, RWLock)
getLockAndBucket ht@(HashTable  _ _ buckets locks) k = do
  let h = hashKey ht k
  lock <- Vector.read locks h
  bucket <- Vector.read buckets h
  return (bucket, lock)

readBucket :: (Ord k, Hashable k) => (HashTable k v) -> k -> (Bucket k v -> IO a) -> IO a
readBucket (HT globLock htRef) k f = do
  Lock.withRead globLock $ do
    ht <- readIORef htRef
    (bucket, lock) <- getLockAndBucket ht k
    Lock.withRead lock $ f bucket

writeBucket :: (Ord k, Hashable k) => (HashTable k v) -> k -> (Bucket k v -> IO a) -> IO a
writeBucket (HT globLock htRef) k f = do
  Lock.withRead globLock $ do
    ht <- readIORef htRef
    (bucket, lock) <- getLockAndBucket ht k
    Lock.withWrite lock $ f bucket

needsSplit :: (Ord k, Hashable k) => Bucket k v -> IO Bool
needsSplit bucket = do
  size <- Bucket.size bucket
  return (size > bucketSplitSize)

lookup :: (Ord k, Hashable k) => (HashTable k v) -> k -> IO (Maybe v)
lookup ht k = readBucket ht k $ flip Bucket.lookup k

insert :: (Ord k, Hashable k) => (HashTable k v) -> k -> v -> IO ()
insert ht k v = do
  willSplit <- insertNoSplit ht k v
  when (willSplit) $ split ht

delete :: (Ord k, Hashable k) => (HashTable k v) -> k -> IO ()
delete ht k = writeBucket ht k $ flip Bucket.delete k

focus :: (Ord k, Hashable k) => HashTable k v -> k -> Strategy v r -> IO r
focus ht k f = do
  (val, willSplit) <- writeBucket ht k (
    \bucket -> do
      ret <- Bucket.focus bucket k f
      s <- needsSplit bucket
      return (ret, s)
    )
  when willSplit $ split ht
  return val

insertNoSplit :: (Ord k, Hashable k) => (HashTable k v) -> k -> v -> IO Bool
insertNoSplit ht k v = do
  writeBucket ht k (
    \bucket-> do
      Bucket.insert bucket k v
      needsSplit bucket
    )

insertNoLock :: (Ord k, Hashable k) => (HashTable_ k v) -> k -> v -> IO ()
insertNoLock ht@(HashTable _ _ buckets _) k v = do
  let h = hashKey ht k
  bucket <- Vector.read buckets h
  Bucket.insert bucket k v

clearBucket :: IOVector (Bucket k v) -> Int -> IO ()
clearBucket buckets i = do
  emptyBucket <- Bucket.new
  Vector.write buckets i emptyBucket

split :: (Ord k, Hashable k) => (HashTable k v) -> IO ()
split (HT globLock htRef) = do
  Lock.acquireWrite globLock
  (HashTable lvl splitPtr buckets locks) <- readIORef htRef
  -- Resize buckets and locks arrays if needed
  let half = 2 ^ (lvl - 1)
  newHt@(HashTable _ _ newBuckets newLocks) <- if (splitPtr+1 >= half)
    then
    do
      let size = 2 ^ lvl
      buckets' <- Vector.grow buckets (size)
      locks' <- Vector.grow locks (size)
      forM_ [size..(2 * size - 1)] (
        \idx -> do
          clearBucket buckets' idx
          Vector.write locks' idx =<< Lock.new
        )
      return (HashTable (succ lvl) 0 buckets' locks')
    else
    do
      return (HashTable lvl (succ splitPtr)  buckets locks)

  -- rehash bucket at old splitPtr
  lock <- Vector.read newLocks splitPtr
  rehashLock <- Vector.read newLocks (splitPtr + half)
  writeIORef htRef newHt
  Lock.withWrite lock $ do
    Lock.withWrite rehashLock $ do
      Lock.releaseWrite globLock
      bucket <- Vector.read newBuckets splitPtr
      clearBucket buckets splitPtr
      _ <- Bucket.forM bucket $ uncurry (insertNoLock newHt)
      return ()
