import json
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from supertable.data_reader import DataReader, Status

from apps.kladnasoft.models import Registration
from supertable.meta_reader import MetaReader
from supertable.rbac.user_manager import UserManager


def get_profile(request):
    user_profile = Registration.objects.get(user=request.user)
    return user_profile


def get_user_hash(super_name, organization, user_name):
    user_manager = UserManager(super_name=super_name, organization=organization)
    user_data = user_manager.get_user_hash_by_name(user_name)
    try:
        return user_data['hash']
    except KeyError:
        raise ValueError("Invalid user data: missing hash field")


import re

def clean_sql_query(query):
    """
    Cleans the SQL query by:
    1. Removing line comments (-- ...)
    2. Removing block comments (/* ... */)
    3. Removing trailing semicolons
    4. Trimming whitespace
    """
    query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)  # Remove line comments
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)  # Remove block comments
    query = re.sub(r';+$', '', query)  # Remove trailing semicolons
    return query.strip()


def apply_limit_safely(query: str, max_rows: int) -> str:
    """
    Ensures the query has a proper LIMIT clause no larger than max_rows + 1.
    Only matches LIMIT when it's a SQL keyword (end of query or before semicolon).
    """
    # Case-insensitive search for LIMIT clause at end of query or before semicolon
    limit_pattern = r'(?<!\w)(limit)\s+(\d+)(?!\w)(?=[^;]*$|;)'
    limit_match = re.search(limit_pattern, query, re.IGNORECASE)

    if limit_match:
        current_limit = int(limit_match.group(2))
        if current_limit > max_rows + 1:
            # Replace existing LIMIT with max_rows + 1
            return re.sub(
                limit_pattern,
                f'LIMIT {max_rows + 1}',
                query,
                flags=re.IGNORECASE,
                count=1  # Only replace first occurrence
            )
        return query
    else:
        # Add LIMIT if none exists
        return f"{query.rstrip(';').strip()} LIMIT {max_rows + 1}"

@login_required
def execute(request):
    import logging
    from supertable.config import defaults
    logging.getLogger('supertable').setLevel(logging.INFO)

    defaults.default.IS_SHOW_TIMING = True

    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            super_name = data.get('super_name')
            query = data.get('query')
            page = int(data.get('page', 1))
            page_size = int(data.get('page_size', 100))
            max_rows = 10000  # Maximum rows to return in total

            if not super_name:
                return JsonResponse({'status': 'error', 'message': 'No table_name provided'}, status=400)

            if not query:
                return JsonResponse({'status': 'error', 'message': 'No query provided'}, status=400)

            # Validate query type (only allow SELECT)
            query = clean_sql_query(query)

            if not query.lower().lstrip().startswith(('select', 'with')):
                return JsonResponse({
                    'status': 'error',
                    'message': 'Only SELECT or WITH (CTE) queries are allowed'
                }, status=400)

            profile = get_profile(request)
            user_hash = get_user_hash(super_name, profile.organization, str(profile.user))

            if not user_hash:
                return JsonResponse({'status': 'error', 'message': 'No user_hash provided'}, status=400)

            # Execute the query with limit if not already present
            safe_query = apply_limit_safely(query, max_rows)


            data_reader = DataReader(
                super_name=super_name,
                organization=profile.organization,
                query=safe_query
            )

            result, status, message = data_reader.execute(user_hash=user_hash)

            if status.value != 'ok':
                return JsonResponse({
                    'status': status.value,
                    'message': message,
                    'result': []
                })

            # Check if we hit the max rows limit
            total_count = result.shape[0]
            if total_count > max_rows:
                result = result.iloc[:max_rows]  # Slice to max_rows
                message = f"Results limited to first {max_rows} rows. {message}"

            # Apply pagination by slicing the DataFrame
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            paginated_result = result.iloc[start_idx:end_idx]

            return JsonResponse({
                'status': status.value,
                'message': message,
                'result': json.loads(paginated_result.to_json(orient='records', date_format='iso')),
                'total_count': min(total_count, max_rows)  # Never return count > max_rows
            })

        except Exception as e:
            return JsonResponse({
                'status': 'error',
                'message': str(e),
                'result': []
            }, status=500)

    # For GET requests, render the template
    return render(request, "execute.html")


from django.views.decorators.http import require_POST

@login_required
@require_POST
def get_schema(request):
    data = json.loads(request.body)
    super_name = data.get('supertable')
    if not super_name:
        return JsonResponse({'status': 'error', 'message': 'No supertable provided'}, status=400)

    profile = get_profile(request)
    user_hash = get_user_hash(super_name, profile.organization, str(profile.user))
    meta_reader = MetaReader(super_name, profile.organization)

    metadata = meta_reader.get_super_meta(user_hash)

    tables = [
        table['name']
        for table in metadata['super'].get('tables', [])
        if not (table['name'].startswith('__') and table['name'].endswith('__'))
    ]

    schema = []
    for table in tables:
        table_schema = meta_reader.get_table_schema(table, user_hash)
        keys = list(table_schema[0].keys())

        schema.append({table: keys})

    context = {
        'schema': schema
    }

    return JsonResponse(context)