import os
import yaml

# directory where the history files are
history_input_dir="/data1/nexus/sdap-ingest-manager/env/.sdap_ingest_manager/tmp/history.0.2.0"
collection_input_conf="collections.yml"
history_output_dir = "history"

with open(collection_input_conf, 'r') as f:
    collections = yaml.load(f, Loader=yaml.FullLoader)

    print('migrating history')
    for (c_id, collection) in collections.items():
        print(f'collection {c_id}')
        input_hist_file = os.path.join(history_input_dir, f"{c_id}.csv")
        output_hist_file = os.path.join(history_output_dir, f"{c_id}.csv")
        output_latestu_file = os.path.join(history_output_dir, f"{c_id}.ts")
        collection_path = os.path.dirname(collection['path'])
        latest_time = 0
        fhi = open(input_hist_file, "r")
        fho = open(output_hist_file, "w")
        for line in fhi:
            line_array = line.split(',')
            filename = line_array[0]
            filepath = os.path.join(collection_path, filename)
            current_time = os.path.getmtime(filepath)
            latest_time = max(latest_time, current_time)
            fho.write(f'{filename},{str(current_time)}\n')
        flu = open(output_latestu_file, "w")
        flu.write(f"{latest_time}")
        flu.close()
        fho.close()
        fhi.close()
