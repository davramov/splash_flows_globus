export $(grep -v '^#' .env | xargs)

# create 'nersc_flow_pool'
prefect work-pool create 'nersc_flow_pool'
prefect work-pool create 'nersc_prune_pool'

# nersc_flow_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "nersc_flow_pool"
prefect deployment build ./orchestration/flows/bl832/nersc.py:nersc_recon_flow -n nersc_recon_flow -p nersc_flow_pool -q nersc_recon_flow_queue
prefect deployment apply nersc_recon_flow-deployment.yaml

# nersc_prune_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "nersc_prune_pool"
prefect deployment build ./orchestration/flows/bl832/prune.py:prune_nersc832_alsdev_pscratch_raw -n prune_nersc832_alsdev_pscratch_raw -p nersc_prune_pool -q prune_nersc832_pscratch_queue
prefect deployment apply prune_nersc832_alsdev_pscratch_raw-deployment.yaml

prefect deployment build ./orchestration/flows/bl832/prune.py:prune_nersc832_alsdev_pscratch_scratch -n prune_nersc832_alsdev_pscratch_scratch -p nersc_prune_pool -q prune_nersc832_pscratch_queue
prefect deployment apply prune_nersc832_alsdev_pscratch_scratch-deployment.yaml
