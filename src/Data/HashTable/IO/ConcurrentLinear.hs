module Data.HashTable.IO.ConcurrentLinear
  (

  ) where

import           Data.IORef
import qualified Control.Concurrent.Lock as Lock
import           Control.Monad.ST (RealWorld)

newtype HashTable k v = HT (IORef (HashTable_ k v))

data HashTable_ k v = HashTable
  { _level :: {-# UNPACK #-} !Int
  , _splitptr :: {-# UNPACK #-} !Int
  , _buckets :: {-# UNPACK #-} !(MutableArray RealWorld (MVar (Bucket RealWorld k v)))
  }


new :: IO (HashTable k v)
