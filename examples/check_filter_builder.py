from examples.defaults import simple_name
from supertable.rbac.filter_builder import FilterBuilder

role_info = {
   "table":
      "facts",
   "columns": "[*]",
   "filters":{
      "AND":[
         {
            "datetime":{
               "range":[
                  {
                     "operation":">=",
                     "value":"2023-01-01 00:00:00.0",
                     "type":"value"
                  },
                  {
                     "operation":"<=",
                     "value":"2023-12-31 23:59:59.999",
                     "type":"value"
                  }
               ]
            }
         },
         {
            "AND":[
               {
                  "CATEGORY":{
                     "operation":"=",
                     "value":"PETCARE",
                     "type":"value"
                  }
               }
            ]
         }
      ]
   }
}

fb = FilterBuilder(simple_name, ["*"], role_info)


print(fb.filter_query)