import os
import json
from pprint import pprint

PAPA_HOME = os.environ["PAPA_HOME"]



# https://stackoverflow.com/questions/20737045/representing-logic-as-data-in-json
 
file = PAPA_HOME + "/refactored/queries/examples/content_model_ex4.json"
file = PAPA_HOME + "/refactored/queries/examples/funnel_summary_ex1.json"
with open(file) as stream:
    q = json.load(stream)

#pprint(q)

file = PAPA_HOME + "/refactored/templates/map.json"
with open(file) as stream:
   ln_map  = json.load(stream)

#pprint(ln_map)


filter_eg = """
filter": {
      "type": "and",
      "fields": [
      { "type": "selector", "dimension": "carrier", "value": "AT&T" },
      { "type": "or", 
        "fields": [
          { "type": "selector", "dimension": "make", "value": "Apple" },
          { "type": "selector", "dimension": "make", "value": "Samsung" }
        ]
      }
    ]
  }
"""


def assembleWhereClause(field_set, prefix="", alias_opt=""):
	
	try:
		op = field_set['type']
		condition = ln_map['sql'][op]
		fields = []
		if op in set(['and','or']):
			
			for field in field_set['fields']:
				fields.append(assembleWhereClause(field,prefix=prefix,alias_opt=alias_opt))
			
			predicate = " ( {} ".format(fields[0])
			for field in fields[1:]:
				predicate += " {} {} ".format(condition,field)
			predicate += " ) "
		else:

			field = prefix + field_set['dimension']
			predicate = " {} {} {} ".format(field,condition,field_set['value'])
		return predicate
	except:
		return ""

	# where_clause = " {} ".format(fields[0])
	# for field in fields[2:]:
	# 	where_clause += " {} {} ".format(op,field)
	# return where_clause
def getWhereClause(q, prefix="", alias_opt=""):
	
	try:
		predicates = q['query']['filter']
		clause  = "{}".format(assembleWhereClause(predicates,prefix=prefix,alias_opt=alias_opt))
		return clause
	except:
		return ""



def getSelectDimsClause(q, prefix="", alias_opt=""):
	
	try:
		dims = q['query']['dimensions']
		clause  = []
		aliases = []
		for dim in dims:
			if alias_opt == "dims":
				name = " AS " + dim
			else:
				name = ""

			col = prefix + dim + name
			clause.append(col)

		return ",".join(clause)
	except:
		return ""

eg_ag = """
"aggregations": [
    { "type": "longSum", "name": "total_usage", "fieldName": "user_count" },
    { "type": "doubleSum", "name": "data_transfer", "fieldName": "data_transfer" }
  ]
"""

def getSelectAgsClause(q, prefix="", alias_opt="default"):
	
	try:
		agrs = q['query']['aggregations']
		
		clause  = []

		for agr in agrs:
			op = agr['type']
			col = prefix + agr['fieldName']
			alias = agr['name']
			if alias_opt == "":
				clause.append("{}".format(alias))
			else:
				col = ln_map['sql'][op].format(col)
				clause.append("{} AS {}".format(col,alias))
		
		return ", ".join(clause)
	except:
		return ""


eg_post_eg = """
  "postAggregations": [
    { "type": "arithmetic",
      "name": "avg_usage",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "fieldName": "data_transfer" },
        { "type": "fieldAccess", "fieldName": "total_usage" }
      ]
    }
  ],
"""
def getSelectPostAgsClause(q, prefix="", alias=True):
	try:
		agrs = q['query']['postAggregations']
		fields = []
		ln = ln_map['sql']
		for agr in agrs:
			op = agr['type']
			if op in set(['arithmetic']):
				left_arg = agr['fields'][0]['fieldName']
				right_arg = agr['fields'][0]['fieldName']
				fn = agr['fn']
				#print(ln[op])
				field = ln[op]['fn'][fn].format(left_arg,right_arg) 
				fields.append(field)
		return ", ".join(fields)
	except:
		return ""

eg_intervel = """
"intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ]
"""

def getWhereInervalClause(q, prefix="", alias_opt="",fieldName="timestamp"):
	
	try:
		intervals = q['query']['intervals']
		fields = []
		ts = prefix + fieldName
		for part in intervals:
			left,right = part.split("/")
			fields.append("({} > {} AND {} < {})".format(ts,left,ts,right))
			
		return ", ".join(fields)
	except:
		return ""

eg_having = """
"having": {
    "type": "greaterThan",
    "aggregation": "total_usage",
    "value": 100
  }
"""

def getHavingClause(q, prefix="", alias=True):
	try:

		fields = q['query']['having']
		clause = []
		
		op = ln_map['sql']['arithmetic']['fn']
		if not isinstance(fields, list):
				fields = [fields]
		
		for field in fields:
			#print(field)
			fn = field['type']
			value = field['value']
			col = field['aggregation']
			clause.append( op[fn].format(col,value))
			
		return ", ".join(clause)	
	except:
		return ""
		
def getDataSource(q):
	return  q['query']["dataSource"]

def getSql(q):
	where_clause  = getWhereClause(q)
	select_dims  = getSelectDimsClause(q)
	select_agg  = getSelectAgsClause(q)
	select_tx  = getSelectPostAgsClause(q)
	where_time  = getWhereInervalClause(q)
	having_clause = getHavingClause(q)
	table_name =  getDataSource(q)


	query = ""
	if select_dims and select_agg:
		query += "SELECT " + select_dims + " , " +  select_agg
	elif select_dims:
		query += "SELECT " + select_dims
	elif select_agg:
		query += "SELECT " + select_agg
	else:
		raise ValueError('query must return something')

	if select_tx:
			query +=  " , " + select_tx
	query += "\n"

	query += "FROM " + table_name + "\n"

	if where_clause and where_time:
		query += "WHERE " + where_clause + " AND " + where_time + "\n"
	elif where_clause:
		query += "WHERE " + where_clause + "\n"
	elif where_time:
		query += "WHERE " + where_time + "\n"
	else:
		pass

	if select_agg:
		query += "GROUP BY " + select_agg + "\n"
	
	if having_clause:
		query += "HAVING " + having_clause + "\n"

	return query


def getCypher(q):


	where_clause  = getWhereClause(q,prefix="x.")
	select_dims_return  = getSelectDimsClause(q)
	select_dims_with  = getSelectDimsClause(q,prefix="x.",alias_opt="dims")
	select_agg_with  = getSelectAgsClause(q,prefix="x.",alias_opt="default")
	select_agg_return  = getSelectAgsClause(q,prefix="",alias_opt="")
	select_tx  = getSelectPostAgsClause(q)
	where_time  = getWhereInervalClause(q,prefix="x.",alias_opt="",fieldName="mytimestamp")
	having_clause = getHavingClause(q)
	table_name = getDataSource(q)

	query = "MATCH (x: {})".format(table_name)
	query += "\n"

	if where_clause and where_time:
		query += "WHERE " + where_clause + " AND " + where_time + "\n"
	elif where_clause:
		query += "WHERE " + where_clause + "\n"
	elif where_time:
		query += "WHERE " + where_time + "\n"
	else:
		pass

	if select_dims_with and select_agg_with:
		query += "WITH " + select_dims_with + " , " +  select_agg_with + "\n"
	elif select_dims_with:
		query += "WITH " + select_dims_with + "\n"
	elif select_agg_with:
		query += "WITH " + select_agg_with + "\n"
	else:
		pass

	found_select = True
	
	if select_dims_return and select_agg_return:
		query += "RETURN " + select_dims_return + " , " +  select_agg_return
	elif select_dims_with:
		query += "RETURN " + select_dims_return
	elif select_agg_with:
		query += "RETURN " + select_agg_return
	else:
		found_select = False

	if not found_select:
		raise ValueError('cypher must return something')
	
	if select_tx:
			query +=  " , " + select_tx

	return query

def getJoin(q,olap="spark-sql"):

	try:
		qtype = q['query']['queryType']
		left = q['query']['left']
		right = q['query']['right']
		on = q['query']['on'][0]
		join_type = q['query']['type']
		name = q['result']['name']
		code_block = "val {} = {}.join({},Seq(\"{}\"),\"{}\")".format(name,left,right,on,join_type)
		return code_block
	except:
		return ""
		
#print("\n** Cypher Query **")
#print(getCypher(q))
#print("\n ** SQL Query**")
#print(getSql(q))