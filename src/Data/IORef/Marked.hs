module Data.IORef.Marked
  (
    MarkedIORef
  , Ticket
  , newIORef
  , readIORef
  , writeIORef
  , readForCAS
  , casIORef
  , peekTicket
  , atomicModifyIORefCAS
  , atomicModifyIORefCAS_
  , markIORef
  , isMarked
  ) where

import           Data.IORef (IORef)
import qualified Data.IORef as IORef
import qualified Data.Atomics as Atomics


data Marked a = Plain {value :: a}
              | Marked {value :: a}

type MarkedIORef a = IORef (Marked a)
type Ticket a = Atomics.Ticket (Marked a)

newIORef :: a -> IO (MarkedIORef a)
newIORef = IORef.newIORef . Plain

readIORef :: MarkedIORef a -> IO a
readIORef r = value <$> IORef.readIORef r

writeIORef :: MarkedIORef a -> a -> IO ()
writeIORef r v = IORef.writeIORef r (Plain v)

readForCAS :: MarkedIORef a -> IO (Ticket a)
readForCAS = Atomics.readForCAS

peekTicket :: Ticket a -> a
peekTicket = value . Atomics.peekTicket

casIORef :: MarkedIORef a -> Ticket a -> a -> IO (Bool, Ticket a)
casIORef r t v = Atomics.casIORef r t (Plain v)

atomicModifyIORefCAS :: MarkedIORef a -> (a -> (a, b)) -> IO b
atomicModifyIORefCAS r f = Atomics.atomicModifyIORefCAS r
  (\wrapped ->
     let (newValue, ret) = f . value $ wrapped
     in (Plain newValue, ret)
  )

atomicModifyIORefCAS_ :: MarkedIORef a -> (a -> a) -> IO ()
atomicModifyIORefCAS_ r f = Atomics.atomicModifyIORefCAS_ r (Plain . f . value)

markIORef :: MarkedIORef a -> IO ()
markIORef r = Atomics.atomicModifyIORefCAS_ r doMark
  where
    doMark :: Marked a -> Marked a
    doMark (Plain v) = Marked v
    doMark (Marked v) = Marked v

isMarked :: MarkedIORef a -> IO Bool
isMarked r = do
  v <- IORef.readIORef r
  case v of
    Plain _ -> return False
    Marked _ -> return True
