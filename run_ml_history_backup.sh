#!/bin/bash
cd /home/carlos/pywork/cqi/products/history_backup/
tar zcvf history_backup_"$(date +"%Y-%m")".tar.gz -C /home/carlos/pywork/cqi history 
ls -1tr | head -n -3 | xargs -d '\n' rm -f --
