module Main where

import System.Environment
import Control.Concurrent

import Server
import Storage

main :: IO ()
main = do
  (port:_) <- getArgs
  putStrLn "Initializing storage."
  storage <- initStorage
  socket <- makeSocket port
  putStrLn $ "Listening on port " ++ port
  cores <- getNumCapabilities
  putStrLn $ "Using cores: " ++ show cores
  mainLoop socket storage
