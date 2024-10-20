from django.http import JsonResponse
from .db_service import fetch_all_from_view

# this, of course, should be improved by adding proper auth, paging, exception handling, logging, etc.
def fetch_orders(request):
    rows = fetch_all_from_view("staging.v_orders")
    response = { "count": len(rows), "data": rows }
    return JsonResponse(response, safe=False)
