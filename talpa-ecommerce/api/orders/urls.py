from django.urls import path
from . import views

urlpatterns = [
    path('', views.fetch_orders, name='fetch_orders'),
]
