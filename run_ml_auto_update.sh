#!/bin/bash
echo "==================$(date) Start ml_auto_update.py================" >> /home/carlos/pywork/cqi/products/log/ml_auto_update.log
python /home/carlos/pywork/cqi/products/_ml_auto_update.py &>> /home/carlos/pywork/cqi/products/log/ml_auto_update.log 
echo "==================$(date) End ml_auto_update.py================" >> /home/carlos/pywork/cqi/products/log/ml_auto_update.log
