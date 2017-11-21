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
import           Control.Concurrent.MVar
import           Control.Concurrent.Lock (Lock)
import qualified Control.Concurrent.Lock as Lock
import           Control.Monad
import           Focus (Strategy)

newtype HashTable k v = HT (MVar (HashTable_ k v))

data HashTable_ k v = HashTable
  { _level :: {-# UNPACK #-} !Int
  , _splitptr :: {-# UNPACK #-} !Int
  , _buckets :: {-# UNPACK #-} !(IOVector (Bucket k v))
  , _locks :: {-# UNPACK #-} !(IOVector Lock)
  }

bucketSplitSize :: Int
bucketSplitSize = 16

fillFactor :: Double
fillFactor = 1.3

createBucketArray :: Int -> IO (IOVector (Bucket k v))
createBucketArray size = Vector.replicateM size Bucket.new

createLockArray :: Int -> IO (IOVector Lock)
createLockArray size = Vector.replicateM size Lock.new

newLeveled :: Int -> IO (HashTable k v)
newLeveled level = do
  let size = 2^level
  buckets <- createBucketArray size
  locks <- createLockArray size
  let ht = HashTable level 0  buckets locks
  ref <- newMVar ht
  return (HT ref)

new :: IO (HashTable k v)
new = newLeveled 1

newSized :: Int -> IO (HashTable k v)
newSized size = do
  let k = fromIntegral $ ceiling (fromIntegral size * fillFactor / fromIntegral bucketSplitSize)
      level = max 1 (fromEnum $ log2 k)
  newLeveled level

hashKey :: (Hashable k) => Int -> Int -> k -> Int
hashKey lvl splitptr k =
  let h = hashAtLvl (lvl-1) k
  in
    if (h < splitptr) then hashAtLvl lvl k else h
  where
    hashAtLvl :: (Hashable k) => Int -> k -> Int
    hashAtLvl l k' =
      let hashcode = hash k'
          mask = 2^l - 1 :: Int
      in
        hashcode .&. mask

lockBucket :: (IOVector Lock) -> Int -> IO Lock
lockBucket locks i = do
  lock <- Vector.read locks i
  Lock.acquire lock
  return lock

lockBucketForKey :: (Ord k, Hashable k) => (HashTable k v) -> k -> IO (Bucket k v, Lock)
lockBucketForKey (HT htRef) k = do
  ht@(HashTable lvl splitPtr buckets locks) <- takeMVar htRef
  let h = hashKey lvl splitPtr k
  lock <- lockBucket locks h
  putMVar htRef ht
  bucket <- Vector.read buckets h
  return (bucket, lock)

withBucket :: (Ord k, Hashable k) => (HashTable k v) -> k -> (Bucket k v -> IO a) -> IO a
withBucket ht k f = do
  (bucket, lock) <- lockBucketForKey ht k
  res <- f bucket
  Lock.release lock
  return res

needsSplit :: (Ord k, Hashable k) => Bucket k v -> IO Bool
needsSplit bucket = do
  size <- Bucket.size bucket
  return (size > bucketSplitSize)

lookup :: (Ord k, Hashable k) => (HashTable k v) -> k -> IO (Maybe v)
lookup ht k = withBucket ht k $ flip Bucket.lookup k

insert :: (Ord k, Hashable k) => (HashTable k v) -> k -> v -> IO ()
insert ht k v = do
  willSplit <- insertNoSplit ht k v
  when (willSplit) $ split ht

delete :: (Ord k, Hashable k) => (HashTable k v) -> k -> IO ()
delete ht k = withBucket ht k $ flip Bucket.delete k

focus :: (Ord k, Hashable k) => HashTable k v -> k -> Strategy v r -> IO r
focus ht k f = do
  (val, willSplit) <- withBucket ht k (
    \bucket -> do
      ret <- Bucket.focus bucket k f
      s <- needsSplit bucket
      return (ret, s)
    )
  when willSplit $ split ht
  return val

insertNoSplit :: (Ord k, Hashable k) => (HashTable k v) -> k -> v -> IO Bool
insertNoSplit ht k v = do
  withBucket ht k (
    \bucket-> do
      Bucket.insert bucket k v
      needsSplit bucket
    )

insertNoLock :: (Ord k, Hashable k) => (HashTable_ k v) -> k -> v -> IO ()
insertNoLock (HashTable lvl splitPtr buckets _) k v = do
  let h = hashKey lvl splitPtr k
  bucket <- Vector.read buckets h
  Bucket.insert bucket k v

clearBucket :: IOVector (Bucket k v) -> Int -> IO ()
clearBucket buckets i = do
  emptyBucket <- Bucket.new
  Vector.write buckets i emptyBucket

split :: (Ord k, Hashable k) => (HashTable k v) -> IO ()
split (HT htRef) = do
  ht@(HashTable lvl splitPtr buckets locks) <- takeMVar htRef
  lock <- Vector.read locks splitPtr
  Lock.acquire lock
  bucket <- Vector.read buckets splitPtr
  clearBucket buckets splitPtr
  -- Resize buckets and locks arrays if needed
  let half = 2 ^ (lvl - 1)
  newHt <- if (splitPtr+1 >= half)
    then
    do
      let size = 2 ^ lvl
      forM_ [0..(size - 1)] (
        \idx ->
          if (idx /= splitPtr)
          then do
            l <- Vector.read locks idx
            Lock.acquire l
          else return ()
        )
      buckets' <- Vector.grow buckets (size)
      locks' <- Vector.grow locks (size)
      forM_ [size..(2 * size - 1)] (
        \idx -> do
          clearBucket buckets' idx
          Vector.write locks' idx =<< Lock.newAcquired
        )
      forM_ (reverse [0..(2 * size - 1)]) (
        \idx ->
          if (idx /= splitPtr)
          then do
            l <- Vector.read locks' idx
            Lock.release l
          else return ()
        )
      return (HashTable (lvl + 1) 0 buckets' locks')
    else
    do
      return ht

  anotherLock <- lockBucket locks (splitPtr + half)
  putMVar htRef newHt
  _ <- Bucket.forM bucket $ uncurry (insertNoLock newHt)
  Lock.release lock
  Lock.release anotherLock
