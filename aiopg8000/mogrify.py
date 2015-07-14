

#https://github.com/datalanche/node-pg-format/blob/master/lib/index.js


import datetime
import re
import binascii
import json
import logging

timetypes = (datetime.datetime,datetime.time,datetime.date)


fmtPattern = {
    'ident': 'I',
    'literal': 'L',
    'string': 's',
}



reservedMap  = {
    "AES128": True,
    "AES256": True,
    "ALL": True,
    "ALLOWOVERWRITE": True,
    "ANALYSE": True,
    "ANALYZE": True,
    "AND": True,
    "ANY": True,
    "ARRAY": True,
    "AS": True,
    "ASC": True,
    "AUTHORIZATION": True,
    "BACKUP": True,
    "BETWEEN": True,
    "BINARY": True,
    "BLANKSASNULL": True,
    "BOTH": True,
    "BYTEDICT": True,
    "CASE": True,
    "CAST": True,
    "CHECK": True,
    "COLLATE": True,
    "COLUMN": True,
    "CONSTRAINT": True,
    "CREATE": True,
    "CREDENTIALS": True,
    "CROSS": True,
    "CURRENT_DATE": True,
    "CURRENT_TIME": True,
    "CURRENT_TIMESTAMP": True,
    "CURRENT_USER": True,
    "CURRENT_USER_ID": True,
    "DEFAULT": True,
    "DEFERRABLE": True,
    "DEFLATE": True,
    "DEFRAG": True,
    "DELTA": True,
    "DELTA32K": True,
    "DESC": True,
    "DISABLE": True,
    "DISTINCT": True,
    "DO": True,
    "ELSE": True,
    "EMPTYASNULL": True,
    "ENABLE": True,
    "ENCODE": True,
    "ENCRYPT": True,
    "ENCRYPTION": True,
    "END": True,
    "EXCEPT": True,
    "EXPLICIT": True,
    "FALSE": True,
    "FOR": True,
    "FOREIGN": True,
    "FREEZE": True,
    "FROM": True,
    "FULL": True,
    "GLOBALDICT256": True,
    "GLOBALDICT64K": True,
    "GRANT": True,
    "GROUP": True,
    "GZIP": True,
    "HAVING": True,
    "IDENTITY": True,
    "IGNORE": True,
    "ILIKE": True,
    "IN": True,
    "INITIALLY": True,
    "INNER": True,
    "INTERSECT": True,
    "INTO": True,
    "IS": True,
    "ISNULL": True,
    "JOIN": True,
    "LEADING": True,
    "LEFT": True,
    "LIKE": True,
    "LIMIT": True,
    "LOCALTIME": True,
    "LOCALTIMESTAMP": True,
    "LUN": True,
    "LUNS": True,
    "LZO": True,
    "LZOP": True,
    "MINUS": True,
    "MOSTLY13": True,
    "MOSTLY32": True,
    "MOSTLY8": True,
    "NATURAL": True,
    "NEW": True,
    "NOT": True,
    "NOTNULL": True,
    "NULL": True,
    "NULLS": True,
    "OFF": True,
    "OFFLINE": True,
    "OFFSET": True,
    "OLD": True,
    "ON": True,
    "ONLY": True,
    "OPEN": True,
    "OR": True,
    "ORDER": True,
    "OUTER": True,
    "OVERLAPS": True,
    "PARALLEL": True,
    "PARTITION": True,
    "PERCENT": True,
    "PLACING": True,
    "PRIMARY": True,
    "RAW": True,
    "READRATIO": True,
    "RECOVER": True,
    "REFERENCES": True,
    "REJECTLOG": True,
    "RESORT": True,
    "RESTORE": True,
    "RIGHT": True,
    "SELECT": True,
    "SESSION_USER": True,
    "SIMILAR": True,
    "SOME": True,
    "SYSDATE": True,
    "SYSTEM": True,
    "TABLE": True,
    "TAG": True,
    "TDES": True,
    "TEXT255": True,
    "TEXT32K": True,
    "THEN": True,
    "TO": True,
    "TOP": True,
    "TRAILING": True,
    "TRUE": True,
    "TRUNCATECOLUMNS": True,
    "UNION": True,
    "UNIQUE": True,
    "USER": True,
    "USING": True,
    "VERBOSE": True,
    "WALLET": True,
    "WHEN": True,
    "WHERE": True,
    "WITH": True,
    "WITHOUT": True,
}





class RawFormatException(Exception):
    pass


class FormatException(Exception):
    def __init__(self, message,format, index, lineno, colno, *args, **kwargs):
        self.format = format


        self.index = index
        self.lineno = lineno
        self.colno = colno

        indent_size = min(len(str(lineno)) + 1 + 1, 4)
        section = self.section = []
        for i, line in enumerate(format.splitlines()):
            if abs(lineno-i) <= 2:
                indent = str(i)
                spaces = indent_size - len(indent)
                spaces = ' '*spaces
                indent += spaces
                section += [indent + line]

        message1 = '{message}\n\nindex: {index}\nline: {lineno}\ncolumn: {colno}\n\n----\n{section}\n----\n'.format(
                                      message=message
                                    , index=self.index
                                    , lineno=self.lineno
                                    , colno=self.colno
                                    , section='\n'.join(self.section))


        super(FormatException,self).__init__(message1,message,format,lineno,colno, {'args': args, 'kwargs': kwargs})

    def __str__(self):
        message = self.args[0]
        for arg in self.args[5:]:
            message += str(arg)
        return message



def formatDate(date):
    date = date.replace('T', ' ')
    date = date.replace('Z', '+00')
    return date
def isReserved(value):
    return value.upper() in reservedMap

pure_identifier_regex = re.compile('^[a-z_][a-z0-9]*$')
def quoteIdent(value):

    if value is None:
        raise RawFormatException('SQL identifier cannot be null or undefined')
    elif isinstance(value,bool) and not value:
        return '"f"'
    elif isinstance(value,bool) and value:
        return '"t"'
    elif isinstance(value,timetypes):
        return '"' + formatDate(value.isoformat()) + '"'
    elif isinstance(value, (bytes,bytearray)):
        raise RawFormatException('SQL identifier cannot be a buffer')
    elif isinstance(value,list):
        return str([quoteIdent(elem) for elem in value])
    elif isinstance(value,dict):
        raise RawFormatException('SQL identifier cannot be an object')
    elif isinstance(value, str):
        pass
    elif isinstance(value, (int,float)):
        value = str(value)
        pass
    else:
        raise RawFormatException('value is of unknown type', type(value), type(value).__name__, value)
    

    # do not quote a valid, unquoted identifier
    if pure_identifier_regex.match(value) is not None and not isReserved(value):
        return value
    

    quoted = ['"']
    for c in value:
        if c == '"':
            quoted += [c,c]
        else:
            quoted += [c]

    quoted += ['"']

    return ''.join(quoted)

def bytes_to_hex_str(buf):
    assert isinstance(buf, (bytes, bytearray))

    hex_buf = binascii.hexlify(buf)
    hex_str = hex_buf.decode('ascii')
    return hex_str

def quoteLiteral(value, explicit_types=False, jsonb=True):
    value0 = value
    if value is None:
        return 'NULL'

    elif isinstance(value,bool):
        return ("'t'" if value else "'f'") + ('::bool' if explicit_types else '')

    elif isinstance(value,datetime.date):
        return "'" + formatDate(value.isoformat()) + "'" + ('::date' if explicit_types else '')
    elif isinstance(value,datetime.datetime):
        return "'" + formatDate(value.isoformat()) + "'" + ('::datetime' if explicit_types else '')
    elif isinstance(value,datetime.time):
        return "'" + formatDate(value.isoformat()) + "'" + ('::time' if explicit_types else '')

    elif isinstance(value, (bytes, bytearray)):
        return "E'\\\\x" + bytes_to_hex_str(value)

    elif isinstance(value, (list,)):
        return 'ARRAY[' + ','.join([quoteLiteral(elem,explicit_types) for elem in value]) +']'

    elif isinstance(value, (dict,)):
        return json.dumps(value) + (('::json' if not jsonb else '::jsonb') if explicit_types else '')

    elif isinstance(value, (str,)):
        pass

    elif isinstance(value, (int,float)):
        value = str(value)
        pass
    else:
        raise RawFormatException('value is of unknown type', type(value), type(value).__name__, value)

    hasBackslash = False
    quoted = ['\'']

    for c in value:
        if c == '\'':
            quoted += [c,c]
        elif c == '\\':
            quoted += [c,c]
            hasBackslash = True
        else:
            quoted += [c]


    quoted += ['\'']
    quoted = ''.join(quoted)

    if hasBackslash:
        return 'E' + quoted

    return quoted

def quoteString(value):
    if value is None:
        return ''

    elif isinstance(value, bool):
        return 'f' if not value else 't'

    elif isinstance(value, timetypes):
        return formatDate(value.isoformat())

    elif isinstance(value, (bytes,bytearray)):
        return '\\x' + bytes_to_hex_str(value)

    elif isinstance(value, list):
        #FIXME, why not include NULLs here?
        return [quoteString(elem) for elem in value
                                  if elem is not None]

    elif isinstance(value,dict):
        return json.dumps(value)

    elif isinstance(value,str):
        return value
    elif isinstance(value, (int,float)):
        value = str(value)
        pass
    else:
        raise RawFormatException('value is of unknown type', type(value), type(value).__name__, value)



def formatWithArray(fmt, parameters):


    regex = '%([%'
    regex += fmtPattern['ident']
    regex += fmtPattern['literal']
    regex += fmtPattern['string']
    regex += '])'
    regex = re.compile(regex)

    result = []
    pointer = 0
    for i,match in enumerate(regex.finditer(fmt)):
        #add stuff since last match
        if pointer < match.start():
            result += [fmt[pointer:match.start()]]
        pointer = match.end()

        match_type = match.group(1)

        if match_type == '%':
            result += ['%']
            continue

        elif match_type == fmtPattern['ident']:
            result += [quoteIdent(parameters[i])]
        elif match_type == fmtPattern['literal']:
            result += [quoteLiteral(parameters[i])]
        elif match_type == fmtPattern['string']:
            result += [quoteString(parameters[i])]

    if pointer < len(fmt):
        result += [fmt[pointer:]]
    return ''.join(result)



def mogrify(format, params={}, explicit_types=False):
    if not isinstance(params, dict):
        raise FormatException('Expected a params dictionary in call to mogrify, got %s instead' % (type(params).__name__)
                                , format=format
                                , index=0
                                , lineno=0
                                , colno=0
                                , params=params )
    result = []
    params_array = []
    cover_index = 0

    #FIXME: dunno why I need this 1 here, it should start at 0
    line_number = 1
    line_index = 0




    regex = r'(%%)|(%\([A-Za-z_][A-Za-z0-9_]*\)s)|(%)|(\n)|(\r\n)|(\r)'
    regex = re.compile(regex)


    for i,match in enumerate(regex.finditer(format, re.MULTILINE)):
        if cover_index < match.start():
            result += [format[cover_index:match.start()]]
        cover_index = match.end()
        full_match = match.group(0)

        if full_match == '%%':
            results += ['%']

        elif full_match == '%':
            raise FormatException('Stray \'%\' character'
                                    , format=format
                                    , index=match.start()
                                    , lineno=line_number
                                    , colno=match.start() - line_index)

        elif full_match in ['\n','\r\n','\n']:
            line_number += 1
            lineindex = match.end()
            result += [full_match]

        elif len(full_match) > 0 and full_match[0] == '%':
            param = full_match[2:-2]
            if param not in params:
                raise FormatException('Expected parameter: %s' % (repr(param),)
                                        , format=format
                                        , index=match.start()
                                        , lineno=line_number
                                        , colno=match.start() - line_index)

            result += [quoteLiteral(params[param])]

        else:
            raise FormatException('Internal error: unknown match'
                                    , format=format
                                    , index=match.start()
                                    , lineno=line_number
                                    , colno=match.start() - line_index
                                    , match=match, full_match=full_match)
    if cover_index < len(format):
        result += [format[cover_index:]]
    return ''.join(result)







if __name__ == '__main__':
    sql = '''
WITH wasted AS(
    SELECT NULL
         , 1
), something AS (
    SELECT *
    FROM stuff
    WHERE a=%L, b=%I, c='%%', e=%s
)

SELECT *
FROM something

'''

    result = formatWithArray(sql, ['1','2','3','4'])


    print (result)

    sql = '''
WITH wasted AS(
    SELECT NULL
         , 1
), something AS (
    SELECT *
    FROM stuff
    WHERE a=%(a)s, b=%(b)s, c=%(c)s, d=%(d)s::jsonb, e=%(e)s
)

SELECT *
FROM something

'''
    try:
        result = mogrify(sql, {'a': datetime.date(1,2,3), 'b': 2, 'c': b'3', 'd': [1,None,'b'], 'e': 'something\\else",\'%%'})

        print (result)
    except:
        logging.exception('Error')