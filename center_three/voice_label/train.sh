#!/bin/bash
nohup python3 -u $PUSHPATH/center_three/voice_label/train_law.py > $PUSHPATH/center_three/voice_label/log.txt 2>&1 &
echo '**' `date +%H:%M:%S` 'start text_classify successfully'