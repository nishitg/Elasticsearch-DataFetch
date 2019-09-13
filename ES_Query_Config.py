required_fields = ["si","imsi","sim_no"]
output_csv_header = ["si","imsi","sim_no"]
output_file_path = "Output.csv"
es_index = "optimus_si_data"
es_type = "fluentd"

es_query = {
  "_source": ["si","imsi","sim_no"],
  "query": {
    "range": {
      "sim_no": {
        "gte" : 899100090031510100,
        "lte" : 899100090032010099

      }
    }
  }
}