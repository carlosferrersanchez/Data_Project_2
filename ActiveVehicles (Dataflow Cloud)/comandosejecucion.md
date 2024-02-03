#Comandos de terminal para ejecutar trans_2

  python3 Trans_2.py \
  --runner DataflowRunner \
  --project edem-dp2 \
  --region europe-west6 \
  --staging_location gs://active_vehicles/staging \
  --temp_location gs://active_vehicles/temp \
  --input_subscription projects/edem-dp2/subscriptions/active-vehicles-sub \
  --output_table edem-dp2:Active_Vehicles.Active_Vehicles \
  --job_name active-vehicles2

  Nota: Hay que ir cambiando el job_name para evitar un conflicto de nombres




  
#Comandos de terminal para ejecutar trans_2

  python3 Trans_2.py \
  --runner DataflowRunner \
  --project edem-dp2 \
  --region europe-west6 \
  --staging_location gs://active_vehicles/staging \
  --temp_location gs://active_vehicles/temp \
  --input_subscription projects/edem-dp2/subscriptions/subs_active_vehicles \
  --output_table edem-dp2:Active_Vehicles.Active_Vehicles \
  --job_name active-vehicles5

  Nota: Hay que ir cambiando el job_name para evitar un conflicto de nombres
