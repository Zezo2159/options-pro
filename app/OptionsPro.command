#!/bin/bash
lsof -ti:5010 | xargs kill -9 2>/dev/null
sleep 1
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 /Applications/OptionsPro.app/Contents/MacOS/OptionsPro &
sleep 2
open http://localhost:5010
