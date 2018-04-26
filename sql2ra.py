import sqlparse
import radb
from sqlparse.sql import IdentifierList, Identifier
import radb.parse
import radb.ast
from radb.ast import RAString
from radb.ast import RelRef
from radb.ast import AttrRef
from sqlparse.tokens import Keyword, DML
from radb.ast import Cross
from radb.ast import Rename
from radb.ast import *
from radb.parse import RAParser as sym
from sqlparse.sql import Where

literals = ["'!='", "'='", "'>'", "'>='", "'<='", "'<>'"]

def create_cross(relations):
    joined_relations = relations[0]
    for i in range (1, len(relations)):
        joined_relations = Cross(joined_relations, relations[i])
    return joined_relations

def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                raise StopIteration
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield str(identifier)
        elif isinstance(item, Identifier):
            yield str(item)
        elif item.ttype is Keyword:
            yield item.value

def extract_table(sql):
    stream = extract_from_part(sql)
    columns = list(extract_table_identifiers(stream))
    tables = []
    for item in columns:
        if " " in str(item):
            table, alias = str(item).split(" ")
            relRef = RelRef(table)
            renameRelRef = Rename(alias, None, relRef)
            tables.append(renameRelRef)
        else:
            relDef = RelRef(str(item))
            tables.append(relDef)


    return tables

def extract_Attributes(token_list):
    attributes = []
    tidx, token = token_list
    if isinstance(token, IdentifierList):
        for item in token.get_identifiers():
            attributes.append(create_Attribute(str(item)))
    else:
        attributes.append(create_Attribute(str(token)))
    return attributes

def create_Attribute(attribute):
    data = attribute.split('.')
    if len(data) == 1:
        return AttrRef(None, attribute.strip())
    elif len(data) > 1:
        return AttrRef(data[0].strip(), data[1].strip())

def parse_select(relations, where_part):
    if not all(where_part):
        return
    idx, cond = where_part
    assert isinstance(cond, Where)
    cond = cond.value.replace('where', '').strip()
    conditions = cond.split("and")
    conditionList = []
    for condition in conditions:
        conditionList.append(create_select(condition))

    joined_conditions = conditionList[0]

    for i in range(1, len(conditionList)):
        joined_conditions = ValExprBinaryOp(joined_conditions, sym.AND, conditionList[i])
    return Select(joined_conditions, relations)



def create_select(cond):
    items = cond.split('=')
    return ValExprBinaryOp(create_Attribute(items[0]), sym.EQ, create_Attribute(items[1]))


def translate(statement):
   # sql = "select distinct A.name, B.name from Eats A, Eats B where A.pizza = B.pizza and B.name = 'Amy'"
    stmt = statement
    attributes = None
    if "*" not in str(stmt):
        tokenlist = stmt.token_next_by(i=sqlparse.sql.TokenList)
        attributes = extract_Attributes(tokenlist)
    #print("attributes" + str(attributes))
    relations = extract_table(statement)
    #print("relations" + str(relations))
    joined_relations = create_cross(relations)
    #print("joined_relations" + str(joined_relations))
    where_part = stmt.token_next_by(i=sqlparse.sql.Where)
    #print("where" + str(where_part))
    select = parse_select(joined_relations, where_part)
    #print("select " + str(select))
    if select is not None:
        if attributes is None:
            project = select
        else:
            project = radb.ast.Project(attributes, select)
    else:
        if attributes is None or not all(attributes):
            project = joined_relations
        else:
            project = radb.ast.Project(attributes, joined_relations)

    print(str(project)+";")
    return (radb.parse.one_statement_from_string(str(project)+";"))



if __name__ == '__main__':
    sqlstring = "select distinct * from Person where age=16 and gender='f'"
    stmt = sqlparse.parse(sqlstring)[0]
    ra = translate(stmt)
