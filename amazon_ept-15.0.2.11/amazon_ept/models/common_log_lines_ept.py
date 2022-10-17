# -*- coding: utf-8 -*-
# See LICENSE file for full copyright and licensing details.

"""
Inherited class to create log lines and relate with the queue log record
"""
from odoo import models, fields


class CommonLogLineEpt(models.Model):
    """
    Inherited class to add common method to create order and product log lines and relate with
    the order queue
    """
    _inherit = "common.log.lines.ept"
    _rec_name = "log_book_id"

    order_queue_data_id = fields.Many2one('shipped.order.data.queue.ept', string='Shipped Order Data Queue')
    fulfillment_by = fields.Selection([('FBA', 'Amazon Fulfillment Network'), ('FBM', 'Merchant Fullfillment Network')],
                                      string="Fulfillment By", help="Fulfillment Center by Amazon or Merchant")
    product_title = fields.Char(string="Product Title", default=False, help="Product Title")

    def amazon_create_product_log_line(self, message, model_id, product_id, default_code, fulfillment_by, log_rec,
                                       product_title='', mismatch=False):
        """
        will creates and product log line
        """
        transaction_vals = {'default_code': default_code,
                            'model_id': model_id,
                            'product_id': product_id and product_id.id or False,
                            'res_id': product_id and product_id.id or False,
                            'message': message,
                            'fulfillment_by': fulfillment_by,
                            'product_title': product_title,
                            'log_book_id': log_rec and log_rec.id or False,
                            'mismatch_details': mismatch}
        log_line = self.create(transaction_vals)
        return log_line

    def amazon_create_order_log_line(self, message, model_id, res_id, order_ref, default_code, fulfillment_by,
                                     log_rec, mismatch=False):
        """
        will creates an order log line
        """
        transaction_vals = {'message': message,
                            'model_id': model_id,
                            'res_id': res_id or False,
                            'order_ref': order_ref,
                            'default_code': default_code,
                            'fulfillment_by': fulfillment_by,
                            'log_book_id': log_rec and log_rec.id or False,
                            'mismatch_details': mismatch}
        log_line = self.create(transaction_vals)
        return log_line
