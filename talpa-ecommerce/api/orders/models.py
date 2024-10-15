from django.db import models

class Order(models.Model):
    order_id = models.BigIntegerField(primary_key=True)
    source = models.CharField(max_length=100)
    customer_id = models.BigIntegerField()
    payment_id = models.BigIntegerField()
    voucher_id = models.BigIntegerField()
    product_id = models.BigIntegerField()
    website = models.CharField(max_length=100)
    order_status = models.CharField(max_length=50)
    voucher_status = models.CharField(max_length=50)
    payment_status = models.CharField(max_length=50)
    order_date = models.DateTimeField()
    payment_date = models.DateTimeField()

    class Meta:
        db_table = 'staging.v_orders'
