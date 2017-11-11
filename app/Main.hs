module Main where

import System.Environment

import Server
import Storage

main :: IO ()
main = do
  (port:_) <- getArgs
  storage <- initStorage
  socket <- makeSocket port
  mainLoop socket storage
