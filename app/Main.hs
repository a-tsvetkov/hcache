module Main where

import System.Environment
import Control.Concurrent
import Network.Socket (withSocketsDo)
import Control.Concurrent.Async

import Server
import Storage

main :: IO ()
main = withSocketsDo $ do
  (port:_) <- getArgs
  putStrLn "Initializing storage."
  storage <- initStorage
  socket <- makeSocket port
  putStrLn $ "Listening on port " ++ port
  cores <- getNumCapabilities
  putStrLn $ "Using cores: " ++ show cores
  wait =<< (async $ mainLoop socket storage)
