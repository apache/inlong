#!/bin/bash

cd ../../
git submodule foreach --recursive git submodule init 
git submodule foreach --recursive git submodule update 
cd -

mkdir build
cd build
cmake ../
make
