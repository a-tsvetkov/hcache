module Data.HashTable.Internal.Utils
  (
    log2
  ) where

log2 :: Floating a => a -> a
log2 = logBase 2.0
