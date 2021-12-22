import boto3
import redis
from json import loads




def run_query(query_string,client):
        try:
            paginator = client.get_paginator('query')
            page_iterator = paginator.paginate(QueryString=query_string)
            for page in page_iterator:
                return _parse_query_result(page)                
        except Exception as err:
            print("Exception while running query:", err)

def _parse_query_result(query_result):
        query_status = query_result["QueryStatus"]

        progress_percentage = query_status["ProgressPercentage"]
        # print(f"Query progress so far: {progress_percentage}%")

        # bytes_scanned = float(query_status["CumulativeBytesScanned"]) / ONE_GB_IN_BYTES
        # print(f"Data scanned so far: {bytes_scanned} GB")

        # bytes_metered = float(query_status["CumulativeBytesMetered"]) / ONE_GB_IN_BYTES
        # print(f"Data metered so far: {bytes_metered} GB")

        column_info = query_result['ColumnInfo']

        # print("Metadata: %s" % column_info)
        # print("Data: ")
        rows = []
        for row in query_result['Rows']:
            #here rows are being printed
            row = _parse_row(column_info, row)
            # print(row)
            rows.append(row)
        return rows
            #print("Next row")

def _parse_row(column_info, row):
        data = row['Data']
        row_output = []
        for j in range(len(data)):
            info = column_info[j]
            datum = data[j]
            row_output.append(_parse_datum(info, datum))

        return "{%s}" % str(row_output)

def _parse_datum(info, datum):
        if datum.get('NullValue', False):
            return "%s=NULL" % info['Name'],

        column_type = info['Type']

        # If the column is of TimeSeries Type
        if 'TimeSeriesMeasureValueColumnInfo' in column_type:
            return _parse_time_series(info, datum)

        # If the column is of Array Type
        elif 'ArrayColumnInfo' in column_type:
            array_values = datum['ArrayValue']
            return "%s=%s" % (info['Name'], _parse_array(info['Type']['ArrayColumnInfo'], array_values))

        # If the column is of Row Type
        elif 'RowColumnInfo' in column_type:
            row_column_info = info['Type']['RowColumnInfo']
            row_values = datum['RowValue']
            return _parse_row(row_column_info, row_values)

        # If the column is of Scalar Type
        else:
            return _parse_column_name(info) + datum['ScalarValue']

def _parse_time_series(info, datum):
        time_series_output = []
        for data_point in datum['TimeSeriesValue']:
            time_series_output.append("{time=%s, value=%s}"
                                      % (data_point['Time'],
                                         _parse_datum(info['Type']['TimeSeriesMeasureValueColumnInfo'],
                                                           data_point['Value'])))
        return "[%s]" % str(time_series_output)

def _parse_array( array_column_info, array_values):
        array_output = []
        for datum in array_values:
            array_output.append(_parse_datum(array_column_info, datum))

        return "[%s]" % str(array_output)
def _parse_column_name(info):
        if 'Name' in info:
            return info['Name'] + "="
        else:
            return ""

