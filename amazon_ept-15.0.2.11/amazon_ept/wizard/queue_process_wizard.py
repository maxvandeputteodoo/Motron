# -*- coding: utf-8 -*-
# Part of Odoo. See LICENSE file for full copyright and licensing details.

"""
Added to create queue or amazon shipped order and process to import orders in odoo.
"""

from odoo import models


class AmazonQueueProcessWizardEpt(models.TransientModel):
    """
    Added to store the shipped order data in queue.
    """
    _name = 'amazon.queue.process.wizard.ept'
    _description = 'Amazon Queue Process Wizard Ept'

    def process_orders_queue_manually(self):
        """
        This method is get the selected queue orders.
        """
        sale_order_obj = self.env['sale.order']
        shipped_order_queue_obj = self.env['shipped.order.data.queue.ept']
        order_queue_ids = self._context.get('active_ids', False)

        common_log_book_obj = self.env['common.log.book.ept']
        log_book_vals = {
            'type': 'import',
            'model_id': self.env['ir.model']._get('shipped.order.data.queue.ept').id,
            'res_id': self.id,
            'module': 'amazon_ept',
            'active': True
        }
        log_book = common_log_book_obj.create(log_book_vals)

        for order_queue_id in order_queue_ids:
            data_queue = shipped_order_queue_obj.browse(order_queue_id)
            sale_order_obj.amz_create_sales_order(data_queue, log_book)
            status = data_queue.shipped_order_data_queue_lines.filtered(lambda x: x.state != 'done')
            if status:
                data_queue.write({'state': 'partially_completed'})
            else:
                data_queue.write({'state': 'completed'})
            self._cr.commit()
        return True
