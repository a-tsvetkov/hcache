module Main where

import System.Environment

import Server
import Storage

main :: IO ()
main = do
  (port:_) <- getArgs
  putStrLn "Initializing storage."
  storage <- initStorage
  socket <- makeSocket port
  putStrLn $ "Listening on port " ++ port
  mainLoop socket storage
