from string import Template

DATE_RANGE_TEMPLATE = Template('''
    {
        "query": {
            "date_utc": {
                "$$gte": "${start_date}T00:00:00.000Z",
                "$$lte": "${end_date}T00:00:00.000Z"
            }
        },
        "options": {
                "page": "${current_page}",
                "limit": "${limit}"
            }
    }
    ''')
