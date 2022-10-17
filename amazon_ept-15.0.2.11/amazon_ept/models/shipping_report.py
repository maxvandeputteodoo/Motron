# -*- coding: utf-8 -*-
# See LICENSE file for full copyright and licensing details.

import time
from datetime import datetime, timedelta
import base64
import csv
from io import StringIO
import pytz
from dateutil import parser
from odoo import models, fields, api, _
from odoo.exceptions import UserError
from odoo.addons.iap.tools import iap_tools
from ..endpoint import DEFAULT_ENDPOINT, DECODE_ENDPOINT
from ..reportTypes import ReportType

utc = pytz.utc
DATE_YMDHMS = "%Y-%m-%d %H:%M:%S"
DATE_YMDTHMS = "%Y-%m-%dT%H:%M:%S"
STOCK_MOVE = 'stock.move'
COMMON_LOG_BOOK_EPT = 'common.log.book.ept'
AMZ_SHIPPING_REPORT_REQUEST_HISTORY = 'shipping.report.request.history'
AMZ_SELLER_EPT = 'amazon.seller.ept'
SALE_ORDER = "sale.order"
IR_ACTION_ACT_WINDOW = 'ir.actions.act_window'
AMZ_INSTANCE_EPT = 'amazon.instance.ept'
COMMON_LOG_LINES_EPT = 'common.log.lines.ept'
RES_PARTNER = 'res.partner'
IR_MODEL = 'ir.model'
VIEW_MODE = 'tree,form'


class ShippingReportRequestHistory(models.Model):
    _name = "shipping.report.request.history"
    _description = "Shipping Report"
    _inherit = ['mail.thread', 'amazon.reports']
    _order = 'id desc'

    @api.depends('seller_id')
    def _compute_company(self):
        """
        Find Company id on change of seller
        :return:  company_id
        """
        for record in self:
            company_id = record.seller_id.company_id.id if record.seller_id else False
            if not company_id:
                company_id = self.env.company.id
            record.company_id = company_id

    def _compute_total_orders(self):
        """
        Get number of orders processed in the report
        :return:
        """
        self.order_count = len(self.amazon_sale_order_ids.ids)

    def _compute_total_moves(self):
        """
        Find all stock moves associated with this report
        :return:
        """
        stock_move_obj = self.env[STOCK_MOVE]
        self.moves_count = stock_move_obj.search_count([('amz_shipment_report_id', '=', self.id)])

    def _compute_total_logs(self):
        """
        Find all stock moves associated with this report
        :return:
        """
        log_obj = self.env[COMMON_LOG_BOOK_EPT]
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        log_ids = log_obj.search([('res_id', '=', self.id), ('model_id', '=', model_id)]).ids
        self.log_count = log_ids.__len__()

        # Set the boolean field mismatch_details as True if found any mismatch details in log lines
        if self.env[COMMON_LOG_LINES_EPT].search_count(
                [('log_book_id', 'in', log_ids), ('mismatch_details', '=', True)]):
            self.mismatch_details = True
        else:
            self.mismatch_details = False

    name = fields.Char(size=256)
    state = fields.Selection([('draft', 'Draft'), ('_SUBMITTED_', 'SUBMITTED'),
                              ('_IN_PROGRESS_', 'IN_PROGRESS'), ('_CANCELLED_', 'CANCELLED'),
                              ('_DONE_', 'DONE'), ('SUBMITTED', 'SUBMITTED'),
                              ('IN_PROGRESS', 'IN_PROGRESS'), ('CANCELLED', 'CANCELLED'),
                              ('IN_QUEUE', 'IN_QUEUE'), ('IN_FATAL', 'IN_FATAL'), ('DONE', 'DONE'),
                              ('partially_processed', 'Partially Processed'), ('_DONE_NO_DATA_', 'DONE_NO_DATA'),
                              ('processed', 'PROCESSED'), ('FATAL', 'FATAL')], string='Report Status', default='draft',
                             help="Report Processing States")
    attachment_id = fields.Many2one('ir.attachment', string="Attachment",
                                    help="Find Shipping report from odoo Attachment")
    seller_id = fields.Many2one(AMZ_SELLER_EPT, string='Seller', copy=False,
                                help="Select Seller id from you wanted to get Shipping report")
    report_request_id = fields.Char(size=256, string='Report Request ID',
                                    help="Report request id to recognise unique request")
    report_document_id = fields.Char(string='Report Document ID',
                                     help="Report document id to recognise unique request")
    report_id = fields.Char(size=256, string='Report ID',
                            help="Unique Report id for recognise report in Odoo")
    report_type = fields.Char(size=256, help="Amazon Report Type")
    start_date = fields.Datetime(help="Report Start Date")
    end_date = fields.Datetime(help="Report End Date")
    requested_date = fields.Datetime(default=time.strftime(DATE_YMDHMS),
                                     help="Report Requested Date")
    company_id = fields.Many2one('res.company', string="Company", copy=False,
                                 compute="_compute_company", store=True)
    user_id = fields.Many2one('res.users', string="Requested User",
                              help="Track which odoo user has requested report")
    amazon_sale_order_ids = fields.One2many(SALE_ORDER, 'amz_shipment_report_id',
                                            string="Sales Order Ids",
                                            help="For list all Orders created while shipment"
                                                 "report process")
    order_count = fields.Integer(compute="_compute_total_orders", store=False,
                                 help="Count number of processed orders")
    moves_count = fields.Integer(compute="_compute_total_moves", string="Move Count", store=False,
                                 help="Count number of created Stock Move")
    log_count = fields.Integer(compute="_compute_total_logs", store=False,
                               help="Count number of created Stock Move")
    is_fulfillment_center = fields.Boolean(default=False,
                                           help="if missing fulfillment center get then set as True")
    mismatch_details = fields.Boolean(compute="_compute_total_logs", help="true if mismatch details found")

    def unlink(self):
        """
        This Method if report is processed then raise UserError.
        """
        for report in self:
            if report.state == 'processed' or report.state == 'partially_processed':
                raise UserError(_('You cannot delete processed report.'))
        return super(ShippingReportRequestHistory, self).unlink()

    @api.constrains('start_date', 'end_date')
    def _check_duration(self):
        """
        Compare Start date and End date, If End date is before start date rate warning.
        @author: Keyur Kanani
        :return:
        """
        if self.start_date and self.end_date < self.start_date:
            raise UserError(_('Error!\nThe start date must be precede its end date.'))
        return True

    @api.model
    def default_get(self, fields):
        """
        Save report type when shipment report created
        @author: Keyur Kanani
        :param fields:
        :return:
        """
        res = super(ShippingReportRequestHistory, self).default_get(fields)
        if fields:
            report_type = ReportType.GET_AMAZON_FULFILLED_SHIPMENTS_DATA
            res.update({'report_type': report_type})
        return res

    def list_of_sales_orders(self):
        """
        List Amazon Sale Orders in Shipment View
        @author: Keyur Kanani
        :return:
        """
        action = {
            'domain': "[('id', 'in', " + str(self.amazon_sale_order_ids.ids) + " )]",
            'name': 'Amazon Sales Orders',
            'view_mode': VIEW_MODE,
            'res_model': SALE_ORDER,
            'type': IR_ACTION_ACT_WINDOW,
        }
        return action

    def list_of_process_logs(self):
        """
        List Shipment Report Log View
        @author: Keyur Kanani
        :return:
        """
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        action = {
            'domain': "[('res_id', '=', " + str(self.id) + " ), ('model_id','='," + str(
                model_id) + ")]",
            'name': 'Shipment Report Logs',
            'view_mode': VIEW_MODE,
            'res_model': COMMON_LOG_BOOK_EPT,
            'type': IR_ACTION_ACT_WINDOW,
        }
        return action

    def list_of_stock_moves(self):
        """
        List All Stock Moves which is generated in a process
        @author: Keyur Kanani
        :return:
        """
        stock_move_obj = self.env[STOCK_MOVE]
        records = stock_move_obj.search([('amz_shipment_report_id', '=', self.id)])
        action = {
            'domain': "[('id', 'in', " + str(records.ids) + " )]",
            'name': 'Amazon FBA Order Stock Move',
            'view_mode': VIEW_MODE,
            'res_model': STOCK_MOVE,
            'type': IR_ACTION_ACT_WINDOW,
        }
        return action

    @api.model
    def create(self, vals):
        """
        Create Sequence for import Shipment Reports
        @author: Keyur Kanani
        :param vals: {}
        :return:
        """
        sequence = self.env.ref('amazon_ept.seq_import_shipping_report_job', raise_if_not_found=False)
        report_name = sequence.next_by_id() if sequence else '/'
        vals.update({'name': report_name})
        return super(ShippingReportRequestHistory, self).create(vals)

    @api.onchange('seller_id')
    def on_change_seller_id(self):
        """
        Set Start and End date of report as per seller configurations
        Default is 3 days
        @author: Keyur Kanani
        """
        if self.seller_id:
            self.start_date = datetime.now() - timedelta(self.seller_id.shipping_report_days)
            self.end_date = datetime.now()

    def create_amazon_report_attachment(self, result):
        """
        Get Shipment Report as an attachment in Shipping reports form view.
        Get Missing Fulfillment Center from attachment file
        If get missing Fulfillment Center then set true value of field is_fulfillment_center
        """
        file_name = "Shipment_report_" + time.strftime("%Y_%m_%d_%H%M%S") + '.csv'
        if self.report_type == "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL":
            result = result.get('document', '')
            result = result.encode()
            result = base64.b64encode(result)
        else:
            result = result.encode()
        attachment = self.env['ir.attachment'].create({
            'name': file_name,
            'datas': result,
            'res_model': 'mail.compose.message',
            'type': 'binary'
        })
        self.message_post(body=_("<b>Shipment Report Downloaded</b>"),
                          attachment_ids=attachment.ids)
        unavailable_fulfillment_center = self.get_missing_fulfillment_center(attachment)
        is_fulfillment_center = False
        if unavailable_fulfillment_center:
            is_fulfillment_center = True
        self.write({'attachment_id': attachment.id, 'is_fulfillment_center': is_fulfillment_center})
        return True

    def check_amazon_report_attachment(self):
        self.ensure_one()
        ir_cron_obj = self.env['ir.cron']
        if not self._context.get('is_auto_process', False):
            ir_cron_obj.with_context({'raise_warning': True}).find_running_schedulers(
                'ir_cron_process_amazon_fba_shipment_report_seller_', self.seller_id.id)

        if not self.attachment_id:
            raise UserError(_("There is no any report are attached with this record."))

    def check_amz_instance_and_shipment(self, row, instance, order_dict, log_rec):
        stock_move_obj = self.env[STOCK_MOVE]
        common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        outbound_history_obj = self.env['shipping.report.order.history']
        if not instance:
            message = 'Skipped Amazon order (%s) because Sales Channel (%s) not found in Odoo. ' % (
                row.get('amazon-order-id'), row.get('sales-channel'))
            common_log_line_obj.amazon_create_order_log_line(message,
                                                             model_id, self.id,
                                                             row.get('amazon-order-id'), False,
                                                             'FBA', log_rec, mismatch=True)
            return True, order_dict
        if row.get('merchant-order-id', False):
            result = outbound_history_obj.verify_outbound_order_processed(row, instance.id)
            if result:
                return True, order_dict

        where_clause = (row.get('shipment-id', False), instance.id, row.get('amazon-order-id', False),
                        row.get('amazon-order-item-id', False).lstrip('0'),
                        row.get('shipment-item-id', False))
        if order_dict.get(where_clause):
            return True, order_dict
        move_found = stock_move_obj.search(
            [('amazon_shipment_id', '=', row.get('shipment-id', False)),
             ('amazon_instance_id', '=', instance.id),
             ('amazon_order_reference', '=', row.get('amazon-order-id', False)),
             ('amazon_order_item_id', '=', row.get('amazon-order-item-id', False).lstrip('0')),
             ('amazon_shipment_item_id', '=', row.get('shipment-item-id', False))])
        if move_found:
            process_invoice = True
            for move in move_found.filtered(lambda x: x.state not in ('done', 'cancel')):
                move.move_line_ids.write({'qty_done': 0.0})
                move._do_unreserve()
                move._action_assign()
                move._set_quantity_done(move.product_uom_qty)
                process_invoice = self.validate_stock_move(move, log_rec, row.get('amazon-order-id'),
                                                           process_invoice)
            wrong_moves = stock_move_obj.search([('amazon_instance_id', '=', instance.id),
                                                 ('amazon_order_reference', '=', row.get('amazon-order-id', False)),
                                                 ('state', 'not in', ['done', 'cancel'])])
            if not wrong_moves:
                invoiced_qty = sum(move_found.sale_line_id.order_id.invoice_ids.mapped('invoice_line_ids').mapped(
                    'quantity'))
                sale_qty = sum(move_found.sale_line_id.order_id.order_line.mapped('product_uom_qty'))
                if invoiced_qty < sale_qty:
                    self.amz_create_and_process_fba_invoices(move_found.sale_line_id.order_id, process_invoice)
            order_dict.update({where_clause: move_found})
            return True, order_dict
        return False, order_dict

    def get_amazon_fulfillment_center_warehouse(self, instance, row, fulfillment_warehouse, skip_orders, log_rec):
        fulfillment_id = row.get('fulfillment-center-id', False)
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
        if fulfillment_id not in fulfillment_warehouse:
            # pass context based on seller is_fulfilment_center_configured or not which help to
            # find fulfillment center warehouse for import order
            is_fulfillment_center = self.seller_id.is_fulfilment_center_configured
            fulfillment_center, fn_warehouse = self.with_context(
                is_fulfillment_center=is_fulfillment_center).get_warehouse(fulfillment_id, instance)
            if not fn_warehouse:
                skip_orders.append(row.get('amazon-order-id', False))
                message = 'Skipped Amazon order %s because Amazon Fulfillment Center not found in Odoo' % (
                    row.get('amazon-order-id', False))
                common_log_line_obj.amazon_create_order_log_line(
                    message, model_id, self.id, row.get('amazon-order-id', False), False, 'FBA', log_rec, mismatch=True)
                return True, {}
            fulfillment_warehouse.update(
                {fulfillment_id: [fn_warehouse, fulfillment_center]})
        warehouse = fulfillment_warehouse.get(fulfillment_id, [False])[0]
        fulfillment_center = fulfillment_warehouse.get(fulfillment_id, [False])[1]
        return False, {'fulfillment_center': fulfillment_center.id, 'warehouse': warehouse.id}

    def process_amazon_shipment_orders(self, outbound_orders_dict, order_details_dict_list, sale_order_list, log_rec):
        """
        Process Amazon Shipment orders for FBA orders or Outbound Orders
        :param outbound_orders_dict: dict {}
        :param order_details_dict_list: dict {}
        :param b2b_order_list:  b2b orders list
        :param log_rec: common log obj
        :return: boolean
        """
        stock_move_obj = self.env[STOCK_MOVE]
        if outbound_orders_dict:
            if self.seller_id.amz_fba_us_program == 'narf':
                self.process_narf_outbound_orders(outbound_orders_dict, log_rec)
            else:
                self.complete_outbound_orders(outbound_orders_dict, log_rec)
        if order_details_dict_list:
            if self.seller_id.is_european_region:
                self.request_and_process_b2b_order_response_ept(order_details_dict_list, sale_order_list, log_rec)
            else:
                self.process_fba_shipment_orders(order_details_dict_list, {}, log_rec, sale_order_list)
        is_partially_processed_report = stock_move_obj.search_count([
            ('amz_shipment_report_id', '=', self.id), ('state', 'not in', ('done', 'cancel'))])
        report_state = 'partially_processed' if is_partially_processed_report else 'processed'
        self.write({'state': report_state})
        if log_rec and not log_rec.log_lines:
            log_rec.unlink()
        return True

    def process_shipment_file(self):
        """
        Process Amazon Shipment File from attachment,
        Import FBA Sale Orders and Sale Order lines for specific amazon Instance
        Test Cases: https://docs.google.com/spreadsheets/d/1IcbZM7o7k4V4DccN3nbR_
        QpXnBBWbztjpglhpNQKC_c/edit?usp=sharing
        @author: Keyur kanani
        :return: True
        """
        self.check_amazon_report_attachment()
        instances = {}
        order_dict = {}
        order_details_dict_list = {}
        outbound_orders_dict = {}
        fulfillment_warehouse = {}
        skip_orders = []
        b2b_order_list = []

        log_rec = self.amz_search_or_create_logs_ept('')
        if self.report_type == "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL":
            imp_file = StringIO(base64.decodebytes(self.attachment_id.datas).decode())
        else:
            imp_file = self.decode_amazon_encrypted_attachments_data(self.attachment_id, log_rec)
        reader = csv.DictReader(imp_file, delimiter='\t')

        for row in reader:
            instance = self.get_instance_shipment_report_ept(row, instances)
            is_exist, order_dict = self.check_amz_instance_and_shipment(row, instance, order_dict, log_rec)
            if is_exist:
                continue
            row.update({'instance_id': instance.id})
            if row.get('amazon-order-id', False) not in skip_orders:
                is_skip, fc_values = self.get_amazon_fulfillment_center_warehouse(instance, row, fulfillment_warehouse,
                                                                                  skip_orders, log_rec)
                if is_skip:
                    continue
                row.update(fc_values)
            if row.get('merchant-order-id', False):
                outbound_orders_dict = self.prepare_amazon_sale_order_line_values(row, outbound_orders_dict)
            else:
                order_details_dict_list = self.prepare_amazon_sale_order_line_values(row, order_details_dict_list)
                if row.get('amazon-order-id', False) and row.get('amazon-order-id', False) not in b2b_order_list:
                    b2b_order_list.append(row.get('amazon-order-id', False))
        self.process_amazon_shipment_orders(outbound_orders_dict, order_details_dict_list, b2b_order_list, log_rec)
        return True

    # def get_amazon_order(self, order_ref, log_rec):
    #     """
    #         Gets Amazon order based on order reference.
    #         :param order_ref: str
    #         :param log_rec: common.log.book.ept
    #         :return: sale.order
    #     """
    #     amz_order = self.search_amazon_outbound_order_ept(order_ref, log_rec)
    #     for sale_order in amz_order:
    #         if amz_order.picking_ids and all([p.state in ('done', 'cancel') for p in sale_order.picking_ids]):
    #             amz_order -= sale_order
    #     return amz_order

    def get_amazon_product(self, instance, sku, log_rec):
        """
            Gets Amazon order based on order reference.
            :param instance: amazon.instance.ept
            :param sku: str
            :param log_rec: common.log.book.ept
            :return amazon.product.ept
        """
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
        amz_product = self.env['amazon.product.ept'].search_amazon_product(instance.id, sku, 'FBA')
        if not amz_product:
            log_line_msg = 'Amazon product {} could not be found for instance {}.'.format(sku, instance.name)
            common_log_line_obj.amazon_create_product_log_line(log_line_msg, model_id, False, sku, 'FBA', log_rec,
                                                               mismatch=True)
        return amz_product

    @staticmethod
    def get_shipment_details(dict_val):
        """
            Gets shipment details from dictionary.
            :param dict_val: dict
            :return: tuple
        """
        return (dict_val.get('shipment-id', ''), dict_val.get('warehouse', False),
                float(dict_val.get('quantity-shipped', 0.0)), dict_val.get('sku', False),
                str(dict_val.get('shipment-item-id', False)))

    @staticmethod
    def should_process_whole_order(product_data, order_lines):
        """
            Decides whether a whole order should be processed at once or not.
            :param product_data: list
            :param order_lines: sale.order.line
        """
        flag = True
        order_line_data = {}
        for line in order_lines:
            if line.product_id.id in order_line_data:
                order_line_data.update(
                    {line.product_id.id: order_line_data.get(line.product_id.id) + line.product_uom_qty})
            else:
                order_line_data.update({line.product_id.id: line.product_uom_qty})
        shipping_data = {}
        for prod_id, data in product_data.items():
            shipped_qty = sum([float(i.get('quantity-shipped', 0.0)) for i in data])
            if prod_id in shipping_data:
                shipping_data.update({prod_id: shipping_data.get(prod_id) + shipped_qty})
            else:
                shipping_data.update({prod_id: shipped_qty})

        for prod_id, qty in order_line_data.items():
            if not (prod_id in list(shipping_data.keys()) and shipping_data.get(prod_id) == order_line_data.get(
                    prod_id)):
                flag = False
                break
        return flag

    # def prepare_pan_eu_orders_dict(self, amz_order, dict_vals, log_rec, order_data, order_lines):
    #     """
    #         Prepares PAN EU outbound orders dict warehouse wise.
    #         :param amz_order: sale.order
    #         :param dict_vals: dict
    #         :param log_rec: common.log.book.ept
    #         :param order_data: dict
    #         :param order_lines: sale.order.line
    #         :return: tuple
    #     """
    #     model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
    #     for dict_val in dict_vals:
    #         shipment_id, fc_warehouse_id, shipped_qty, product_sku, ship_item_id = self.get_shipment_details(dict_val)
    #         amz_product = self.get_amazon_product(amz_order.amz_instance_id, product_sku, log_rec)
    #         if not amz_product:
    #             continue
    #         product = amz_product.product_id
    #         ord_line = amz_order.order_line.filtered(lambda ol, product=product: ol.product_id.id == product.id)
    #         if not ord_line:
    #             skip_reason = 'Skipped an order line because product named {} is not available.'.format(product.name)
    #             self.env[COMMON_LOG_LINES_EPT].create_log_lines(skip_reason, model_id, self, log_rec)
    #             continue
    #         if fc_warehouse_id not in list(order_data.keys()):
    #             order_data.update({fc_warehouse_id: {product.id: [dict_val]}})
    #         else:
    #             product_data = order_data.get(fc_warehouse_id)
    #             if product.id in product_data.keys():
    #                 product_data.update({product.id: product_data.get(product.id) + [dict_val]})
    #             else:
    #                 product_data.update({product.id: [dict_val]})
    #     if amz_order.warehouse_id.id in list(order_data.keys()):
    #         shipment_data = order_data.get(amz_order.warehouse_id.id)
    #         order_data.pop(amz_order.warehouse_id.id)
    #         order_data.update({amz_order.warehouse_id.id: shipment_data})
    #     return amz_order, dict_vals, log_rec, order_data, order_lines

    def get_amz_new_order_name(self, amazon_order, seq):
        """
            Creates new name for new Amazon order.
            :param amazon_order: sale.order
            :param seq: int
            :return: str
        """
        new_name = amazon_order.name + '/' + str(seq)
        if self.env[SALE_ORDER].search([('name', '=', new_name)]):
            seq += 1
            return self.get_amz_new_order_name(amazon_order, seq)
        return new_name

    # def get_historical_or_create_new_order(self, amz_order, warehouse, log_rec, product_data):
    #     """
    #         Gets historical order or creates new order.
    #         :param amz_order: sale.order
    #         :param warehouse: int
    #         :param log_rec: common.log.book.ept
    #         :param product_data: list
    #         :return: tuple
    #     """
    #     flag = True
    #     orders = self.search_amazon_outbound_order_ept(amz_order.amz_order_reference, log_rec)
    #     historical_order = orders.filtered(lambda o: o.warehouse_id.id == warehouse)
    #     if historical_order:
    #         new_order = historical_order
    #     else:
    #         if self.should_change_warehouse(amz_order, product_data):
    #             flag = False
    #             new_order = amz_order
    #             new_order.warehouse_id = warehouse
    #             new_order.onchange_warehouse_id()
    #         else:
    #             new_name = self.get_amz_new_order_name(amz_order, 1)
    #             new_order = amz_order.copy(default={'name': new_name, 'order_line': None, 'warehouse_id': warehouse})
    #             new_order.onchange_warehouse_id()
    #     return new_order, flag

    # def should_change_warehouse(self, amz_order, product_data):
    #     """
    #         Checks whether it should change warehouse of an order or create new order to process remaining order lines.
    #         :param amz_order: sale.order
    #         :param product_data: list
    #         :return: bool
    #     """
    #     lines_without_zero_qty = amz_order.order_line.filtered(lambda ol: ol.product_uom_qty != 0.0)
    #     return self.should_process_whole_order(product_data, lines_without_zero_qty)

    # def prepare_amz_update_stock_move_vals(self, order, shipment_vals):
    #     """
    #         Prepares stock move values.
    #         :param order: sale.order
    #         :param shipment_vals: list
    #         :return: dict
    #     """
    #     return {
    #         'amazon_shipment_id': ','.join(list(set([val.get('shipment-id') for val in shipment_vals]))),
    #         'amazon_shipment_item_id': ','.join([val.get('shipment-item-id') for val in shipment_vals]),
    #         'amazon_order_item_id': ','.join([val.get('amazon-order-item-id') for val in shipment_vals]),
    #         'amazon_order_reference': order.amz_order_reference,
    #         'amazon_instance_id': order.amz_instance_id.id,
    #         'tracking_number': ','.join(list(set([val.get('tracking-number') for val in shipment_vals]))),
    #         'amz_shipment_report_id': self.id
    #     }

    # def amz_update_stock_move(self, line, shipment_vals):
    #     """
    #         Updates existing move or creates new stock move.
    #         :param line: sale.order.line
    #         :param shipment_vals: list
    #         :return: None
    #     """
    #     move = line.move_ids.filtered(lambda m: m.state not in ['done', 'cancel'] and not m.amazon_shipment_id)
    #     move = move.filtered(lambda m: m.picking_id.state not in ['done', 'cancel'] and
    #                                    m.picking_id.location_dest_id.usage == 'customer')
    #     move_vals = self.prepare_amz_update_stock_move_vals(line.order_id, shipment_vals)
    #     move._action_assign()
    #     move._set_quantity_done(sum([float(val.get('quantity-shipped', 0.0)) for val in shipment_vals]))
    #     move.write(move_vals)

    # def update_amz_stock_move_ept(self, order, product_data):
    #     """
    #         Updates Amazon stock move.
    #         :param order: sale.order
    #         :param product_data:
    #         :return: None
    #     """
    #     pickings = order.picking_ids.filtered(lambda p: p.state not in ['done', 'cancel'])
    #     for move in pickings.move_lines.filtered(lambda m: m.state not in ['done', 'cancel']):
    #         move._action_assign()
    #         move._set_quantity_done(move.product_uom_qty)
    #     order.picking_ids.filtered(lambda p: p.state not in ['done', 'cancel']).move_lines._action_done()
    #     for product_id, ship_data in product_data.items():
    #         line = order.order_line.filtered(lambda ol, product_id=product_id: ol.product_id.id == product_id)
    #         move = line.move_ids.filtered(
    #             lambda m: m.state == 'done' and m.picking_id.location_dest_id.usage == 'customer'
    #                       and not m.amazon_shipment_id)
    #         move_vals = self.prepare_amz_update_stock_move_vals(line.order_id, ship_data)
    #         move.write(move_vals)

    # def process_pan_eu_outbound_dict(self, amz_order, order_lines, order_data, orders_to_process, log_rec):
    #     """
    #         Processes Prepared PAN EU outbound dict.
    #         :param amz_order: sale.order
    #         :param order_lines: sale.order.line
    #         :param order_data: dict
    #         :param orders_to_process: sale.order
    #         :param log_rec: common.log.book.ept
    #         :return: tuple
    #     """
    #     should_cancel = []
    #     for warehouse, product_data in order_data.items():
    #         if warehouse == amz_order.warehouse_id.id:
    #             amz_order.filtered(lambda o: o.state in ['draft', 'sent']).action_confirm()
    #             for product_id, shipment_data in product_data.items():
    #                 line = order_lines.filtered(lambda ol, product_id=product_id: ol.product_id.id == product_id and
    #                                                                               ol.product_uom_qty != 0.0)
    #                 self.amz_update_stock_move(line, shipment_data)
    #             amz_order.picking_ids.filtered(lambda p: p.state not in ['done', 'cancel']).move_lines._action_done()
    #             self.amazon_check_back_order_ept(amz_order)
    #             orders_to_process |= amz_order
    #         else:
    #             if len(list(order_data.keys())) == 1 and self.should_process_whole_order(product_data, order_lines):
    #                 amz_order.warehouse_id = warehouse
    #                 amz_order.onchange_warehouse_id()
    #                 amz_order.filtered(lambda o: o.state in ['draft', 'sent']).action_confirm()
    #                 self.update_amz_stock_move_ept(amz_order, product_data)
    #                 orders_to_process |= amz_order
    #             else:
    #                 new_order, flag = self.get_historical_or_create_new_order(amz_order, warehouse, log_rec,
    #                                                                           product_data)
    #                 should_cancel = self.amz_prepare_pan_eu_outbound_cancel_lines(product_data, order_lines, new_order,
    #                                                                               should_cancel, amz_order, flag)
    #                 new_order.order_line.filtered(lambda ol: ol.product_uom_qty == 0.0).unlink()
    #                 new_order.action_confirm()
    #                 self.update_amz_stock_move_ept(new_order, product_data)
    #                 orders_to_process |= new_order
    #     self.cancel_remaining_pickings(should_cancel)
    #     return amz_order, order_lines, order_data, orders_to_process

    # def amz_prepare_pan_eu_outbound_cancel_lines(self, product_data, order_lines, new_order, should_cancel, amz_order,
    #                                              flag):
    #     """
    #     Prepare list of should be cancelled product dictionary while processing outbound orders from shipping report
    #     :param product_data: dict{}
    #     :param order_lines: list[]
    #     :param new_order: sale.order()
    #     :param should_cancel: list[]
    #     :param amz_order: sale.order()
    #     :param flag: boolean
    #     :return: list[]
    #     """
    #     if flag:
    #         for product_id, shipment_data in product_data.items():
    #             done_qty = sum([float(val.get('quantity-shipped', 0.0)) for val in shipment_data])
    #             lines = order_lines.filtered(lambda ol, product_id=product_id: ol.product_id.id == product_id)
    #             for line in lines:
    #                 amz_extra_values = self.amz_prepare_outbound_order_line_extra_vals(line)
    #                 if 0 < line.product_uom_qty <= done_qty:
    #                     line.copy(default={'order_id': new_order.id, 'product_uom_qty': line.product_uom_qty,
    #                                        **amz_extra_values})
    #                     done_qty -= line.product_uom_qty
    #                     line.product_uom_qty -= line.product_uom_qty
    #                 elif line.product_uom_qty > 0:
    #                     line.copy(default={'order_id': new_order.id, 'product_uom_qty': done_qty, **amz_extra_values})
    #                     line.product_uom_qty -= done_qty
    #         if amz_order.state not in ['draft', 'sent']:
    #             should_cancel.append(amz_order)
    #     return should_cancel

    # def amz_prepare_outbound_order_line_extra_vals(self, line):
    #     """
    #     Prepares extra values for sale order line in order to use them in outbound orders.
    #     :param line: sale.order.line
    #     :return: dict
    #     """
    #     return {}

    # def process_pan_eu_outbound_orders(self, outbound_orders_dict, log_rec):
    #     """
    #         Processes outbound orders for PAN EU program enabled sellers.
    #         @author: Sunil Khatri
    #         :param outbound_orders_dict: dict
    #         :param log_rec: common.log.book.ept
    #         :return: None
    #     """
    #     model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
    #     sale_order_obj = self.env[SALE_ORDER]
    #     for order_ref, dict_vals in outbound_orders_dict.items():
    #         orders_to_process, amz_order = sale_order_obj, self.get_amazon_order(order_ref, log_rec)
    #         if not amz_order:
    #             continue
    #         order_lines = amz_order.order_line.filtered(lambda ol: ol.product_type != 'service')
    #         if not order_lines:
    #             skip_reason = 'Skipped an order named {} because order lines with non service type products' \
    #                           ' were not found.'.format(amz_order.name)
    #             log_line = self.env[COMMON_LOG_LINES_EPT].create_log_lines(skip_reason, model_id, self, log_rec)
    #             log_line.mismatch_details = True
    #             continue
    #         order_data = {}
    #
    #         # Prepares outbound orders dictionary warehouse wise.
    #         amz_order, dict_vals, log_rec, order_data, order_lines = \
    #             self.prepare_pan_eu_orders_dict(amz_order, dict_vals, log_rec, order_data, order_lines)
    #
    #         # Processes prepared dictionary.
    #         amz_order, order_lines, order_data, orders_to_process = \
    #             self.process_pan_eu_outbound_dict(amz_order, order_lines, order_data, orders_to_process, log_rec)
    #
    #         # Binds Amazon details to orders and then creates invoices as per workflow.
    #         if orders_to_process:
    #             orders_to_process.write({'amz_shipment_report_id': self.id})
    #             self.bind_amazon_data(orders_to_process.order_line, dict_vals, log_rec)
    #             self.amz_create_invoices_as_per_workflow(orders_to_process, model_id, log_rec)
    #
    #         # Deletes order lines which have 0 quantity and deletes sale order which has no order line.
    #         amz_order.order_line.filtered(lambda ol: ol.product_uom_qty == 0.0).unlink()
    #         if not amz_order.order_line:
    #             self.env['sale.order.cancel'].create({'order_id': amz_order.id}).action_cancel()
    #             amz_order.unlink()
    #         self.env.cr.commit()

    # def amz_create_invoices_as_per_workflow(self, orders, model_id, log_rec):
    #     """
    #         Creates invoices of sale order as per workflow.
    #         :param orders: sale.order
    #         :param model_id: int
    #         :param log_rec: common.log.book.ept
    #         :return: None
    #     """
    #     for order in orders:
    #         auto_workflow_process_id = order.auto_workflow_process_id
    #         if not auto_workflow_process_id:
    #             auto_workflow_process_id = order.amz_seller_id.fba_auto_workflow_id
    #             order.auto_workflow_process_id = auto_workflow_process_id and auto_workflow_process_id.id
    #         try:
    #             order.validate_and_paid_invoices_ept(auto_workflow_process_id)
    #         except Exception as ex:
    #             self.env[COMMON_LOG_LINES_EPT].create_log_lines(ex, model_id, self, log_rec)

    # @staticmethod
    # def cancel_remaining_pickings(amazon_orders):
    #     """
    #         Cancels an existing picking and creates new one.
    #         :param amazon_orders: list
    #         :return: None
    #     """
    #     for amazon_order in list(set(amazon_orders)):
    #         if amazon_order.picking_ids:
    #             amazon_order.picking_ids.filtered(lambda p: p.state not in ['done', 'cancel']).action_cancel()
    #             amazon_order.action_confirm()

    # def amazon_check_back_order_ept(self, order):
    #     """
    #         Checks for back order.
    #         :param order: sale.order
    #         :return: None
    #     """
    #     back_order_obj = self.env['stock.backorder.confirmation']
    #     if order.picking_ids._check_backorder():
    #         picking = order.picking_ids.filtered(lambda x: x.state not in ['done', 'cancel', 'confirmed'])
    #         if not picking:
    #             order.picking_ids.filtered(lambda x: x.state == 'confirmed').move_lines._action_assign()
    #         else:
    #             back_order_obj.with_context(default_pick_ids=[(4, p.id) for p in picking],
    #                                         button_validate_picking_ids=[p.id for p in picking]).create({}).process()
    #             back_order = order.picking_ids.filtered(
    #                 lambda p: p.state not in ['done', 'cancel'] and p.backorder_id.id)
    #             if back_order:
    #                 back_order.move_lines._action_assign()

    # def bind_amazon_shipment_ept(self, picking, value, carrier_id):
    #     if not picking.amazon_shipment_id:
    #         values = self.prepare_outbound_picking_update_vals_ept(value, carrier_id, value.get('tracking-number', ''))
    #         picking.write(values)
    #     if picking.carrier_tracking_ref:
    #         if value.get('tracking-number', '') not in picking.carrier_tracking_ref.split(','):
    #             picking.carrier_tracking_ref = picking.carrier_tracking_ref + ',' + value.get('tracking-number', '')
    #     else:
    #         picking.carrier_tracking_ref = value.get('tracking-number', '')

    # def bind_amazon_data(self, order_lines, dict_vals, log_rec):
    #     """
    #         Binds amazon data to stock picking and stock moves on processed orders.
    #         :param log_rec:
    #         :param order_lines: sale.order.line
    #         :param dict_vals: dict
    #         :return: bool
    #     """
    #     for value in dict_vals:
    #         shipment_id, fc_warehouse, shipped_qty, sku, ship_item_id = self.get_shipment_details(value)
    #         amz_product = self.get_amazon_product(order_lines.order_id.amz_instance_id, sku, log_rec)
    #         if not amz_product:
    #             continue
    #         product = amz_product.product_id
    #         line = order_lines.filtered(
    #             lambda ol, product=product, fc_warehouse=fc_warehouse: ol.product_id.id == product.id and
    #                                                                    ol.order_id.warehouse_id.id == fc_warehouse)
    #         move = line.move_ids.filtered(lambda m, shipment_id=shipment_id: m.state == 'done' and
    #                                                                          shipment_id in m.amazon_shipment_id.split(
    #             ','))
    #         move.fulfillment_center_id = value.get('fulfillment_center', '')
    #         move.filtered(lambda m: m.state not in ['done', 'cancel'])._action_assign()
    #         carrier_id = False
    #         if value.get('carrier', ''):
    #             carrier_id = self.env[SALE_ORDER].get_amz_shipping_method(
    #                 value.get('carrier', ''), line.order_id.amz_seller_id.shipment_charge_product_id)
    #         picking = move.picking_id.filtered(lambda p: not p.backorder_id) or move.picking_id
    #         self.bind_amazon_shipment_ept(picking, value, carrier_id)

    @staticmethod
    def validate_stock_move(move, job, order_name, process_invoice):
        """
            Validates stock move.
        """
        try:
            move._action_done()
        except Exception as exception:
            process_invoice = False
            log_line_vals = {'message': 'Stock move is not done of order %s Due to %s' % (order_name, exception),
                             'fulfillment_by': 'FBA', 'mismatch_details': True}
            transaction_log_lines = [(0, 0, log_line_vals)]
            job.write({'log_lines': transaction_log_lines})
        return process_invoice

    def amz_create_and_process_fba_invoices(self, order, process_invoice):
        """
        Create invoices as per workflow configuration for FBA Shipped orders
        :param order:sale.order
        :param process_invoice: Boolean
        :return: boolean
        """
        fba_auto_workflow_id = order.amz_seller_id.fba_auto_workflow_id
        if fba_auto_workflow_id.create_invoice and process_invoice:
            # For Update Invoices in Amazon, we have to create Invoices as per Shipment id
            shipment_ids = {}
            for move in order.order_line.move_ids:
                if move.amazon_shipment_id in shipment_ids:
                    shipment_ids.get(move.amazon_shipment_id).append(move.amazon_shipment_item_id)
                else:
                    shipment_ids.update({move.amazon_shipment_id: [move.amazon_shipment_item_id]})
            for shipment, shipment_item in list(shipment_ids.items()):
                to_invoice = order.order_line.filtered(lambda l: l.qty_to_invoice != 0.0)
                if to_invoice:
                    self.create_process_fba_invoices(shipment_item, order, fba_auto_workflow_id)
        return True

    def create_process_fba_invoices(self, shipment_item, order, fba_auto_workflow_id):
        """
        Create Invoices and process it according to auto invoice workflow for FBA orders
        :param shipment_item: list[]
        :param order: sale.order()
        :param fba_auto_workflow_id: workflow if
        :return:
        """
        invoices = order.with_context({'shipment_item_ids': shipment_item})._create_invoices()
        invoice = invoices.filtered(lambda l: l.line_ids)
        if invoice:
            order.validate_invoice_ept(invoice)
            if fba_auto_workflow_id.register_payment:
                order.paid_invoice_ept(invoice)
        else:
            for inv in invoices:
                if not inv.line_ids:
                    inv.unlink()

    def get_amazon_b2b_orders(self, wrapper_obj):
        orders = []
        if not isinstance(wrapper_obj.get('Orders', []), list):
            orders.append(wrapper_obj.get('Orders', {}))
        else:
            orders = wrapper_obj.get('Orders', [])
        return orders

    def request_and_process_b2b_order_response_ept(self, order_details_dict_list, b2b_order_list, log_rec):
        """
        We will call ListOrderItems API
        :param order_details_dict_list:
        :param b2b_order_list:
        :param log_rec:
        :return:
        """
        if not b2b_order_list:
            return {}

        kwargs = self.prepare_amazon_request_report_kwargs(self.seller_id)
        kwargs.update({'emipro_api': 'get_amazon_orders_sp_api'})
        for x in range(0, len(b2b_order_list), 50):
            sale_orders_list = b2b_order_list[x:x + 50]
            sale_orders_list = ",".join(sale_orders_list)
            amz_b2b_order_dict = {}
            kwargs.update({'sale_order_list': sale_orders_list})

            response = iap_tools.iap_jsonrpc(DEFAULT_ENDPOINT, params=kwargs, timeout=1000)
            result = response.get('result', {}) if isinstance(response.get('result', {}), list) else [
                response.get('result', {})]

            for wrapper_obj in result:
                orders = self.get_amazon_b2b_orders(wrapper_obj)
                for order in orders:
                    amazon_order_ref = order.get('AmazonOrderId', False)
                    if not amazon_order_ref:
                        continue
                    amz_b2b_order_dict.update({amazon_order_ref: {
                        'IsBusinessOrder': order.get('IsBusinessOrder', {}),
                        'IsPrime': order.get('IsPrime', {})}})
            self.process_fba_shipment_orders(order_details_dict_list, amz_b2b_order_dict, log_rec, sale_orders_list)
        return True

    def process_narf_outbound_orders(self, outbound_order_data, job):
        """
        Process for create outbound orders for NARF program enabled customers
        :param outbound_order_data: dict
        :param job: common connector log
        :return:
        """
        for order_ref, lines in outbound_order_data.items():
            amazon_order = self.search_amazon_outbound_order_ept(order_ref, job)
            if not amazon_order:
                continue
            # prepare lines as per shipment id
            picking = amazon_order.picking_ids.filtered(lambda x: x.state not in ['done', 'cancel'])
            if not picking:
                continue
            shipment_dict = self.prepare_outbound_shipment_dict(lines)
            for shipment_id, shipment_lines in shipment_dict.items():
                picking = amazon_order.picking_ids.filtered(lambda x: x.state not in ['done', 'cancel'])
                if not picking:
                    continue
                self.process_narf_outbound_shipment_lines(shipment_lines, amazon_order, picking, shipment_id, job)
                self.amazon_fba_shipment_report_workflow(amazon_order, job)
        return True

    def process_narf_outbound_shipment_lines(self, shipment_lines, amazon_order, picking, shipment_id, job):
        """
        Process Outbound orders shipment lines,
        Set tracking references in order pickings.
        :param shipment_lines: list of lines
        :param amazon_order: sale.order()
        :param picking: stock.picking()
        :param shipment_id: amazon shipment id (string)
        :param job: common log book obj
        :return:
        """
        order_obj = self.env[SALE_ORDER]
        prod_dict, track_list, track_num = {}, [], ''
        for ship_line in shipment_lines:
            if ship_line.get('tracking-number', False) and ship_line.get('tracking-number', False) not in track_list:
                track_list.append(ship_line.get('tracking-number', False))
                track_num = ship_line.get('tracking-number', False) if not track_num else \
                    track_num + ',' + ship_line.get('tracking-number', False)
            prod_dict = self.prepare_outbound_product_dict_ept(amazon_order, ship_line, prod_dict, job)
        for product, shipped_qty in prod_dict.items():
            stock_move_line = picking.move_line_ids.filtered(lambda x, product=product: x.product_id.id == product.id)
            if not stock_move_line:
                stock_move = picking.move_lines.filtered(lambda x, product=product: x.product_id.id == product.id)
                sml_vals = self.prepare_outbound_stock_move_line_vals(product, picking, shipped_qty, stock_move)
                stock_move_line = stock_move_line.create(sml_vals)
            else:
                stock_move_line.move_id._set_quantity_done(float(shipped_qty))
            sm_vals = self.prepare_outbound_stock_move_update_vals(shipment_id, amazon_order, ship_line, track_num)
            stock_move_line.move_id.write(sm_vals)
            carrier_id = False
            if ship_line.get('carrier', ''):
                # shipment_charge_product_id is fetched according to seller wise
                carrier_id = order_obj.get_amz_shipping_method(
                    ship_line.get('carrier', ''), amazon_order.amz_seller_id.shipment_charge_product_id)
            if not picking.amazon_shipment_id:
                pick_vals = self.prepare_outbound_picking_update_vals_ept(ship_line, carrier_id, track_num)
                picking.write(pick_vals)

    def prepare_outbound_product_dict_ept(self, amazon_order, ship_line, prod_dict, job):
        """
        Calculate total shipped quantity of a product in the current report file.
        Create processed order line history in shipping_report_order_history table.
        :param amazon_order:sale.order()
        :param ship_line: dict{}
        :param prod_dict: dict{}
        :param job: common.log.book.ept()
        :return: dict{}
        """
        amazon_product_obj = self.env["amazon.product.ept"]
        common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        shipped_qty = float(ship_line.get("quantity-shipped", 0.0))
        product_sku = ship_line.get("sku", False)
        amazon_product = amazon_product_obj.search_amazon_product(amazon_order.amz_instance_id.id, product_sku, "FBA")
        if not amazon_product:
            message = "Amazon Product[%s] not found for Instance[%s] in ERP." % (
                product_sku, amazon_order.amz_instance_id.name)
            common_log_line_obj.amazon_create_order_log_line(message, model_id, self.id,
                                                             amazon_order.amz_order_reference, False, 'FBA', job,
                                                             mismatch=True)
        else:
            product = amazon_product.product_id
            if product not in prod_dict:
                prod_dict.update({product: shipped_qty})
            else:
                prod_dict.update({product: prod_dict.get(product) + shipped_qty})
            self.create_outbound_order_history_ept(amazon_order.amz_instance_id.id, ship_line)
        return prod_dict

    def create_outbound_order_history_ept(self, amz_instance_id, ship_line):
        """
        Create record for outbound order history, while reprocess this records will be searched.
        :param amz_instance_id: amazon.instance.ept()
        :param ship_line: dict{}
        :return: shipping.report.order.history()
        """
        outbound_history_obj = self.env['shipping.report.order.history']
        vals = {
            'instance_id': amz_instance_id,
            'amazon_order_ref': ship_line.get('amazon-order-id', False),
            'order_line_ref': ship_line.get('amazon-order-item-id', False),
            'shipment_id': ship_line.get('shipment-id', False),
            'shipment_line_id': ship_line.get('shipment-item-id', False)
        }
        return outbound_history_obj.create(vals)

    @staticmethod
    def prepare_outbound_shipment_dict(lines):
        """
        Process all lines and group it with shipment-id.
        @author: Keyur Kanani
        :param lines: list[{},{}]
        :return: dict{'shipment-id':[{line1},{line2}]}
        """
        shipment_dict = {}
        for line in lines:
            if line.get('shipment-id', False) not in shipment_dict:
                shipment_dict.update({line.get('shipment-id', False): [line]})
            else:
                shipment_dict.update(
                    {line.get('shipment-id', False): shipment_dict.get(line.get('shipment-id', False)) + [line]})
        return shipment_dict

    def search_amazon_outbound_order_ept(self, order_ref, job):
        """
        Search Outbound order with amazon order reference and if found in draft or sent state then confirm order.
        :param order_ref:
        :param job: common connector log
        :return: sale.order()
        """
        order_obj = self.env[SALE_ORDER]
        common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        amazon_order = order_obj.search([("amz_order_reference", "=", order_ref), ("amz_is_outbound_order", "=", True),
                                         ('amz_seller_id', '=', self.seller_id.id)])
        if not amazon_order:
            message = "Order %s not found in ERP." % (order_ref)
            common_log_line_obj.amazon_create_order_log_line(message, model_id, self.id, order_ref, False, 'FBA',
                                                             job, mismatch=True)
            return amazon_order
        if amazon_order.filtered(lambda x: x.state in ["draft", "sent"] and x.amz_seller_id.amz_fba_us_program == 'narf'):
            amazon_order.action_confirm()
        return amazon_order

    @staticmethod
    def prepare_outbound_picking_update_vals_ept(ship_line, carrier_id, track_num):
        """
        Prepare values for update outbound orders detail in picking
        :param ship_line: dict{}
        :param carrier_id: delivery carrier id
        :param track_num: tracking number (If multiple numbers then comma separated string)
        :return:dict{}
        """
        vals = {
            "amazon_shipment_id": ship_line.get("shipment-id", False),
            "is_fba_wh_picking": True,
            "fulfill_center": ship_line.get("fulfillment_center", False),
            "updated_in_amazon": True,
            "carrier_id": carrier_id,
            "carrier_tracking_ref": track_num
        }
        if ship_line.get('estimated-arrival-date', False):
            estimated_arrival = parser.parse(ship_line.get('estimated-arrival-date', '')).astimezone(utc).strftime(
                DATE_YMDHMS)
            vals.update({'estimated_arrival_date': estimated_arrival})
        return vals

    def prepare_outbound_stock_move_update_vals(self, shipment_id, amazon_order, ship_line, track_num):
        """
        Preparing amazon order values to write in stock moves.
        :param shipment_id:
        :param amazon_order: sale.order()
        :param ship_line: dict{}
        :param track_num: 'tracking_number' / 'track_num1,track_num2'
        :return: dict{}
        """
        return {
            "amazon_shipment_id": shipment_id,
            "amazon_instance_id": amazon_order.amz_instance_id.id,
            "amazon_order_reference": ship_line.get('merchant-order-id', False),
            "amazon_order_item_id": ship_line.get("amazon-order-item-id", False),
            "amazon_shipment_item_id": ship_line.get("shipment-item-id", False),
            "tracking_number": track_num,
            "fulfillment_center_id": ship_line.get("fulfillment_center", ''),
            "amz_shipment_report_id": self.id
        }

    @staticmethod
    def prepare_outbound_stock_move_line_vals(product, picking, shipped_qty, stock_move):
        """
        Prepare stock move line values for force create if not available.
        @author: Keyur Kanani
        :param product: product.product()
        :param picking: stock.picking()
        :param shipped_qty: shipping done qty
        :param stock_move: stock.move()
        :return: {}
        """
        return {
            'product_id': product.id,
            'product_uom_id': product.uom_id.id,
            'picking_id': picking.id,
            'qty_done': float(shipped_qty) or 0,
            'location_id': picking.location_id.id,
            'location_dest_id': picking.location_dest_id.id,
            'move_id': stock_move.id
        }

    def get_instance_shipment_report_ept(self, row, instances):
        """
        Find Instance from data of shipment report
        :param row: dict{}
        :param instances: dict of instances
        :return: {}
        """
        sale_order_obj = self.env[SALE_ORDER]
        marketplace_obj = self.env['amazon.marketplace.ept']
        if row.get('sales-channel', '') == "Non-Amazon":
            order = sale_order_obj.search([("amz_order_reference", "=", row.get('merchant-order-id', "")),
                                           ("amz_is_outbound_order", "=", True)])
            instance = order.amz_fulfillment_instance_id
        elif row.get('sales-channel', '') not in instances:
            instance = marketplace_obj.find_instance(self.seller_id, row.get('sales-channel', ''))
            instances.update({row.get('sales-channel', ''): instance})
        else:
            instance = instances.get(row.get('sales-channel', ''))
        return instance

    def get_amazon_outbound_order(self, order_ref, job):
        """
        Get an already created outbound orders from sale orders.
        :param order_ref: amazon order references.
        :param job: common log book object
        :return: sale.order()
        """
        order_obj = self.env[SALE_ORDER]
        common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        amazon_order = order_obj.search([("amz_order_reference", "=", order_ref), ("amz_is_outbound_order", "=", True),
                                         ('amz_seller_id', '=', self.seller_id.id)])
        if not amazon_order:
            message = "Order %s not found in ERP." % (order_ref)
            common_log_line_obj.amazon_create_order_log_line(
                message, model_id, self.id, order_ref, False, 'FBA', job, mismatch=True)
            return False

        amazon_order = amazon_order.filtered(lambda x: x.state == "sale")
        if not amazon_order:
            message = "Order %s is already updated in ERP." % (order_ref)
            common_log_line_obj.amazon_create_order_log_line(
                message, model_id, self.id, order_ref, False, 'FBA', job)
        return amazon_order

    def prepare_warehouse_wise_outbound_dict(self, lines):
        """
        This method will prepare shipment lines dict by same warehouse.
        @author: Maulik Barad on Date 3-Mar-2022.
        """
        shipment_dict = {}
        for line in lines:
            if line.get('warehouse', False) not in shipment_dict:
                shipment_dict.update({line.get('warehouse', False): [line]})
            else:
                shipment_dict.update(
                    {line.get('warehouse', False): shipment_dict.get(line.get('warehouse', False)) + [line]})
        return shipment_dict

    def complete_outbound_orders(self, outbound_order_data, log_book):
        """
        Completes the outbound orders by processing the shipping report data.
        @author: Maulik Barad on Date 3-Mar-2022.
        """
        amazon_product_obj = self.env["amazon.product.ept"]
        warehouse_obj = self.env["stock.warehouse"]
        common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
        new_pickings = self.env["stock.picking"]
        model_id = common_log_line_obj.get_model_id(self._name)

        for order_ref, lines in outbound_order_data.items():
            order = self.get_amazon_outbound_order(order_ref, log_book)
            if not order:
                continue
            picking = order.picking_ids.filtered(lambda x: x.state not in ['done', 'cancel'])
            if not picking:
                continue
            shipment_dict = self.prepare_warehouse_wise_outbound_dict(lines)
            for warehouse_id, shipment_lines in shipment_dict.items():
                location_id = location_dest_id = False
                warehouse = warehouse_obj.browse(warehouse_id)
                diff_warehouse = bool(picking.picking_type_id.warehouse_id.id != warehouse_id)
                if diff_warehouse:
                    location_id = warehouse.lot_stock_id.id
                    location_dest_id = picking.location_dest_id.id
                    out_picking_type = warehouse.out_type_id
                    new_picking = order.picking_ids.filtered(lambda x:
                                                             x.picking_type_id == out_picking_type and
                                                             x.location_id.id == location_id and
                                                             x.amazon_shipment_id == shipment_lines[0].get(
                                                                 "shipment-id"))
                else:
                    new_picking = picking
                move_lines = picking.move_lines.filtered(lambda x: x.state not in ["done", "cancel"])
                if not move_lines:
                    continue
                for line in shipment_lines:
                    product_sku = line.get("sku", False)
                    amazon_product = amazon_product_obj.search_amazon_product(order.amz_instance_id.id, product_sku,
                                                                              "FBA")
                    if not amazon_product:
                        message = "Amazon Product[%s] not found for Instance[%s] in ERP." % (product_sku,
                                                                                             order.amz_instance_id.name)
                        common_log_line_obj.amazon_create_order_log_line(message, model_id, self.id, order_ref, False,
                                                                         "FBA", log_book, mismatch=True)
                        continue

                    move_lines = picking.move_lines.filtered(lambda x: x.state not in ["done", "cancel"])
                    if not move_lines:
                        continue

                    existing_move = order.picking_ids.move_lines.filtered(
                        lambda x:
                        x.product_id == amazon_product.product_id and
                        x.amazon_shipment_item_id == line.get("shipment-item-id") and
                        x.fulfillment_center_id.id == line.get("fulfillment_center") and
                        x.state == "done")
                    if existing_move:
                        continue

                    move = move_lines.filtered(lambda x: x.product_id == amazon_product.product_id)
                    if move:
                        if diff_warehouse and not new_picking:
                            new_picking = picking.copy({
                                "move_lines": [],
                                "picking_type_id": out_picking_type.id,
                                "state": "draft",
                                "origin": _("%s", order.name),
                                "location_id": location_id,
                                "location_dest_id": location_dest_id,
                                "amazon_shipment_id": False,
                                "is_fba_wh_picking": False,
                                "fulfill_center": '',
                                "updated_in_amazon": False,
                                "carrier_id": False,
                                "carrier_tracking_ref": ''
                            })
                        new_pickings |= self.generate_and_validate_outbound_stock_move(line, diff_warehouse, move,
                                                                                       new_picking, order, location_id,
                                                                                       location_dest_id)

                if new_picking:
                    self.update_outbound_picking(line, order, new_picking, new_pickings)
            if all([p.state in ('done', 'cancel') for p in order.picking_ids]):
                order.validate_and_paid_invoices_ept(order.auto_workflow_process_id)
        return True

    def generate_and_validate_outbound_stock_move(self, line, diff_warehouse, move, new_picking, order, location_id,
                                                  location_dest_id):
        """
        This method will validate the move as per the shipment line data.
        @author: Maulik Barad on Date 8-Mar-2022.
        """
        vals_to_update = {"amazon_shipment_id": line.get("shipment-id", False),
                          "amazon_instance_id": order.amz_instance_id.id,
                          "amazon_order_reference": line.get('merchant-order-id', False),
                          "amazon_order_item_id": line.get("amazon-order-item-id", False),
                          "amazon_shipment_item_id": line.get("shipment-item-id", False),
                          "tracking_number": line.get("tracking-number", ''),
                          "fulfillment_center_id": line.get("fulfillment_center", ''),
                          "amz_shipment_report_id": self.id}
        shipped_qty = float(line.get("quantity-shipped", 0.0))
        if diff_warehouse:
            vals_to_update.update({"picking_id": new_picking.id,
                                   "location_id": location_id,
                                   "location_dest_id": location_dest_id})
            if move.product_uom_qty == shipped_qty:
                new_move = move.copy(vals_to_update)
                move._action_cancel()
            else:
                if move.state == "done":
                    defaults = move._prepare_move_split_vals(shipped_qty)
                    new_move_vals = move.copy_data(defaults)[0]
                else:
                    new_move_vals = move._split(shipped_qty)[0]
                new_move_vals.update(vals_to_update)
                new_move = move.create(new_move_vals)

            new_move._action_assign()
            new_move._set_quantity_done(shipped_qty)
            new_move.move_line_ids.sorted()._action_done()
            new_move.write({'state': 'done', 'date': fields.Datetime.now()})
        else:
            if move.product_uom_qty != shipped_qty:
                new_move_vals = move._split(move.product_uom_qty - shipped_qty)[0]
                new_move = move.create(new_move_vals)
                new_move._action_confirm(merge=False)
            vals_to_update.update({'state': 'done', 'date': fields.Datetime.now()})
            move._set_quantity_done(shipped_qty)
            move.move_line_ids.sorted()._action_done()
            move.write(vals_to_update)
        return new_picking

    def update_outbound_picking(self, line, order, picking, new_pickings):
        """
        Method to update the outbound picking after updating the moves and validating them.
        @author: Maulik Barad on Date 9-Mar-2022.
        """
        carrier_id = False
        if line.get('carrier', ''):
            carrier_id = self.env[SALE_ORDER].get_amz_shipping_method(
                line.get('carrier', ''), order.amz_seller_id.shipment_charge_product_id)
        if new_pickings and picking in new_pickings and not picking.amazon_shipment_id:
            picking.write({
                "amazon_shipment_id": line.get("shipment-id", False),
                "is_fba_wh_picking": True,
                "fulfill_center": line.get("fulfillment_center", ''),
                "updated_in_amazon": True,
                "carrier_id": carrier_id,
                "carrier_tracking_ref": line.get("tracking-number", '')
            })
        return True

    # @api.model
    # def process_outbound_orders(self, outbound_order_data, job):
    #     """
    #     Processes the outbound shipment data from shipping report as of Multi Channel Sale order.
    #     @param outbound_order_data: Data of outbound orders.
    #     @param job: Record of common log book.
    #     """
    #     order_obj = self.env[SALE_ORDER]
    #     amazon_product_obj = self.env["amazon.product.ept"]
    #     common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
    #     model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
    #
    #     for order_ref, lines in outbound_order_data.items():
    #         available_orders = remain_order = order_obj
    #         amazon_order = self.get_amazon_outbound_order(order_ref, job)
    #         if not amazon_order:
    #             continue
    #         all_lines = amazon_order.order_line.filtered(lambda x: x.product_id.type == "product")
    #         for line in lines:
    #             fulfillment_warehouse_id = line.get("warehouse", False)
    #             shipped_qty = float(line.get("quantity-shipped", 0.0))
    #             product_sku = line.get("sku", False)
    #             amazon_product = amazon_product_obj.search_amazon_product(amazon_order.amz_instance_id.id,
    #                                                                       product_sku, "FBA")
    #             if not amazon_product:
    #                 message = "Amazon Product[%s] not found for Instance[%s] in ERP." % (
    #                     product_sku, amazon_order.amz_instance_id.name)
    #                 common_log_line_obj.amazon_create_order_log_line(message, model_id, self.id,
    #                                                                  order_ref, False, 'FBA', job, mismatch=True)
    #                 continue
    #             product = amazon_product.product_id
    #             existing_order_line = amazon_order.order_line.filtered(
    #                 lambda x, product=product: x.product_id.id == product.id)
    #             if amazon_order.warehouse_id.id == fulfillment_warehouse_id:
    #                 if existing_order_line.product_uom_qty != shipped_qty:
    #                     new_quantity = existing_order_line.product_uom_qty - shipped_qty
    #                     if remain_order:
    #                         existing_order_line.copy(default={'order_id': remain_order.id,
    #                                                           'product_uom_qty': new_quantity})
    #                     else:
    #                         remain_order = self.split_order(
    #                             {(amazon_order, fulfillment_warehouse_id): {
    #                                 existing_order_line: new_quantity}})
    #                     existing_order_line.product_uom_qty = shipped_qty
    #
    #                 existing_order_line.amazon_order_item_id = line.get("amazon-order-item-id", False)
    #                 available_orders |= amazon_order
    #             else:
    #                 splitted_order = order_obj.browse()
    #                 for order in available_orders:
    #                     if order.warehouse_id.id == fulfillment_warehouse_id:
    #                         splitted_order = order
    #                         break
    #
    #                 if len(all_lines) == 1 and existing_order_line.product_uom_qty == shipped_qty:
    #                     amazon_order.warehouse_id = fulfillment_warehouse_id
    #                     existing_order_line.amazon_order_item_id = line.get("amazon-order-item-id", False)
    #                     available_orders |= amazon_order
    #
    #                 elif len(all_lines) > 1:
    #                     if splitted_order:
    #                         new_order_line = existing_order_line.copy(
    #                             default={'order_id': splitted_order.id,
    #                                      'product_uom_qty': shipped_qty})
    #                     else:
    #                         new_order = self.split_order({(amazon_order, fulfillment_warehouse_id):
    #                                                           {existing_order_line: shipped_qty}})
    #                         new_order_line = new_order.order_line.filtered(
    #                             lambda x, product=product: x.product_id.id == product.id)
    #                         available_orders |= new_order
    #
    #                     new_order_line.amazon_order_item_id = line.get("amazon-order-item-id", False)
    #                     existing_order_line.product_uom_qty -= shipped_qty
    #
    #                     if existing_order_line.product_uom_qty:
    #                         if remain_order:
    #                             existing_order_line.copy( \
    #                                 default={'order_id': remain_order.id,
    #                                          'product_uom_qty': existing_order_line.product_uom_qty})
    #                         else:
    #                             remain_order = self.split_order(
    #                                 {(amazon_order, fulfillment_warehouse_id): {
    #                                     existing_order_line: existing_order_line.product_uom_qty}})
    #                         existing_order_line.product_uom_qty = 0
    #
    #                 elif len(all_lines) == 1 and existing_order_line.product_uom_qty != shipped_qty:
    #                     new_quantity = existing_order_line.product_uom_qty - shipped_qty
    #                     amazon_order.warehouse_id = fulfillment_warehouse_id
    #
    #                     if remain_order:
    #                         existing_order_line.copy(default={'order_id': remain_order.id,
    #                                                           'product_uom_qty': new_quantity})
    #                     else:
    #                         remain_order = self.split_order(
    #                             {(amazon_order, fulfillment_warehouse_id): {
    #                                 existing_order_line: new_quantity}})
    #                     existing_order_line.product_uom_qty = shipped_qty
    #                     existing_order_line.amazon_order_item_id = line.get("amazon-order-item-id", False)
    #                     available_orders |= amazon_order
    #
    #             if existing_order_line.product_uom_qty == 0:
    #                 existing_order_line.unlink()
    #         if not amazon_order.order_line:
    #             available_orders -= amazon_order
    #             amazon_order.unlink()
    #         if available_orders:
    #             available_orders.action_confirm()
    #             self.attach_amazon_data(available_orders.order_line, lines)
    #             self.amazon_fba_shipment_report_workflow(available_orders, job)
    #     return True

    # def attach_amazon_data(self, order_lines, order_line_data):
    #     """
    #     Match order line with data and attach all amazon order and shipment data with stock picking
    #     and moves.
    #     @author: Maulik Barad on Date 27-Jan-2019.
    #     @param order_lines: All order lines.
    #     @param order_line_data: Order line and shipment data.
    #     """
    #     for data in order_line_data:
    #         order_line = order_lines.filtered(lambda x, data=data:
    #                                           x.amazon_order_item_id == data.get("amazon-order-item-id", False) and
    #                                           x.product_uom_qty == float(data.get("quantity-shipped", 0.0)) and
    #                                           x.order_id.warehouse_id.id == data.get("warehouse", False))
    #         move = order_line.move_ids.filtered(lambda x: x.state in ["confirmed", "assigned"])
    #         move.write({
    #             "amazon_shipment_id": data.get("shipment-id", False),
    #             "amazon_instance_id": order_line.order_id.amz_instance_id.id,
    #             "amazon_order_reference": data.get('merchant-order-id', False),
    #             "amazon_order_item_id": data.get("amazon-order-item-id", False),
    #             "amazon_shipment_item_id": data.get("shipment-item-id", False),
    #             "tracking_number": data.get("tracking-number", ''),
    #             "fulfillment_center_id": data.get("fulfillment_center", ''),
    #             "amz_shipment_report_id": self.id
    #         })
    #         move._action_assign()
    #         move._set_quantity_done(float(data.get("quantity-shipped", 0.0)))
    #
    #         carrier_id = False
    #         if data.get('carrier', ''):
    #             # shipment_charge_product_id is fetched according to seller wise
    #             carrier_id = self.env[SALE_ORDER].get_amz_shipping_method( \
    #                 data.get('carrier', ''), order_line.order_id.amz_seller_id.shipment_charge_product_id)
    #
    #         picking = move.picking_id
    #         if not picking.amazon_shipment_id:
    #             picking.write({
    #                 "amazon_shipment_id": data.get("shipment-id", False),
    #                 "is_fba_wh_picking": True,
    #                 "fulfill_center": data.get("fulfillment_center", ''),
    #                 "updated_in_amazon": True,
    #                 "carrier_id": carrier_id,
    #                 "carrier_tracking_ref": data.get("tracking-number", '')
    #             })
    #     return True

    def prepare_move_data_ept(self, amazon_order, line):
        """
        Prepare Stock Move data for FBA Process
        @author: Keyur Kanani
        :param amazon_order: sale.order()
        :param line: csv line dictionary
        :return: {}
        """
        return {
            'amazon_shipment_id': line.get('shipment-id', False),
            'amazon_instance_id': amazon_order.amz_instance_id.id,
            'amazon_order_reference': line.get('amazon-order-id', False),
            'amazon_order_item_id': line.get('amazon-order-item-id', False).lstrip('0'),
            'amazon_shipment_item_id': line.get('shipment-item-id', False),
            'tracking_number': line.get('tracking-number', ''),
            'fulfillment_center_id': line.get('fulfillment_center', ''),
            'amz_shipment_report_id': self.id,
            'product_uom_qty': line.get('quantity-shipped', 0.0)
        }

    # @api.model
    # def copy_amazon_order(self, amazon_order, warehouse):
    #     """
    #     Duplicate the amazon Orders
    #     @author: Keyur Kanani
    #     :param amazon_order: sale.order()
    #     :param warehouse: int
    #     :return: sale.order()
    #     """
    #     if not amazon_order.amz_instance_id.seller_id.is_default_odoo_sequence_in_sales_order_fba:
    #         new_name = self.get_order_sequence(amazon_order, 1)
    #         new_sale_order = amazon_order.copy(default={'name': new_name, 'order_line': None,
    #                                                     'warehouse_id': warehouse})
    #     else:
    #         new_sale_order = amazon_order.copy(default={'order_line': None, 'warehouse_id': warehouse})
    #     new_sale_order.onchange_warehouse_id()
    #     return new_sale_order

    # @api.model
    # def split_order(self, split_order_line_dict):
    #     """
    #     Split Amazon Order
    #     @author: Keyur Kanani
    #     :param split_order_line_dict: {}
    #     :return:
    #     """
    #     order_obj = self.env[SALE_ORDER]
    #     new_orders = order_obj.browse()
    #     for order, lines in split_order_line_dict.items():
    #         order_record = order[0]
    #         warehouse = order[1]
    #         new_amazon_order = self.copy_amazon_order(order_record, warehouse)
    #         for line, shipped_qty in lines.items():
    #             line.copy(default={'order_id': new_amazon_order.id, 'product_uom_qty': shipped_qty})
    #             new_orders += new_amazon_order
    #     return new_orders

    @api.model
    def get_order_sequence(self, amazon_order, order_sequence):
        """
        Get Order sequence according to seller configurations
        @author: Keyur Kanani
        :param amazon_order: sale.order()
        :param order_sequence:
        :return:
        """
        order_obj = self.env[SALE_ORDER]
        new_name = "%s%s" % (amazon_order.amz_instance_id.seller_id.order_prefix or '',
                             amazon_order.amz_order_reference)
        new_name = new_name + '/' + str(order_sequence)
        if order_obj.search([('name', '=', new_name)]):
            order_sequence = order_sequence + 1
            return self.get_order_sequence(amazon_order, order_sequence)
        return new_name

    def find_and_unlink_pending_amaozn_orders(self, instance, order_ref, pending_orders_dict):
        sale_order_obj = self.env[SALE_ORDER]
        pending_order = pending_orders_dict.get((order_ref, instance.id))
        if not pending_order:
            sale_order = sale_order_obj.search([('amz_order_reference', '=', order_ref),
                                                ('amz_instance_id', '=', instance.id),
                                                ('amz_fulfillment_by', '=', 'FBA'),
                                                ('state', '=', 'draft'),
                                                ('is_fba_pending_order', '=', True)])
            if sale_order:
                pending_orders_dict.update({(order_ref, instance.id): sale_order.ids})
                sale_order.unlink()
        return True

    def process_fba_shipment_orders(self, order_details_dict_list, amz_b2b_order_dict, job, sale_orders_list):
        """
        Create Sale Orders, order lines and Shipment lines, giftwrap etc..
        Create and Done Stock Move.
        @author: Keyur Kanani
        :param order_details_dict_list: {}
        :return boolean: True
        """
        sale_order_obj = self.env[SALE_ORDER]
        sale_order_line_obj = self.env['sale.order.line']
        amz_instance_obj = self.env[AMZ_INSTANCE_EPT]
        stock_location_obj = self.env['stock.location']
        pending_orders_dict = {}
        partner_dict = {}
        product_details = {}
        commit_flag = 1
        country_dict = {}
        state_dict = {}
        customers_location = stock_location_obj.search([('usage', '=', 'customer'),
                                                        '|', ('company_id', '=', self.seller_id.company_id.id),
                                                        ('company_id', '=', False)], limit=1)
        for order_ref, lines in order_details_dict_list.items():
            if order_ref not in sale_orders_list:
                continue

            amz_order_list = []
            skip_order, product_details = self.prepare_amazon_products(lines, product_details, order_ref, job)
            if skip_order:
                continue
            b2b_order_vals = amz_b2b_order_dict.get(order_ref, {})
            for order_line in lines:
                order_line.update(b2b_order_vals)
                instance = amz_instance_obj.browse(order_line.get('instance_id', False))
                # # If pending order then unlink that order and create new order
                self.find_and_unlink_pending_amaozn_orders(instance, order_ref, pending_orders_dict)
                # Search or create customer
                partner_dict, country_dict, state_dict = self.search_or_create_partner(order_line, instance,
                                                                                       partner_dict, country_dict,
                                                                                       state_dict)
                amazon_order = sale_order_obj.create_amazon_shipping_report_sale_order(order_line, partner_dict,
                                                                                       self.id)
                if amazon_order not in amz_order_list:
                    amz_order_list.append(amazon_order)
                # Create Sale order lines
                so_lines = sale_order_line_obj.create_amazon_sale_order_line(amazon_order, order_line, product_details)
                move_data_dict = self.prepare_move_data_ept(amazon_order, order_line)
                so_line = so_lines.filtered(lambda l: l.product_type != 'service')
                self.amazon_fba_stock_move(so_line, customers_location, move_data_dict)
            self.amazon_fba_shipment_report_workflow(amz_order_list, job)
            if commit_flag == 10:
                self.env.cr.commit()
                commit_flag = 0
            commit_flag += 1
        return True

    def amazon_fba_shipment_report_workflow(self, amz_order_list, job):
        """
        The function is used for create Invoices and Process Stock Move done.
        :param amz_order_list: sale.order()
        :return:
        """
        stock_move_obj = self.env[STOCK_MOVE]
        for order in amz_order_list:
            stock_moves = stock_move_obj.search(
                [('amazon_order_reference', '=', order.amz_order_reference),
                 ('amazon_instance_id', '=', order.amz_instance_id.id)])
            process_invoice = True
            for move in stock_moves:
                process_invoice = self.validate_stock_move(move, job, order.name, process_invoice)
            order.write({'state': 'sale'})
            self.amz_create_and_process_fba_invoices(order, process_invoice)
        return True

    def amazon_fba_stock_move(self, order_line, customers_location, move_vals):
        """
        Create Stock Move according to MRP module and bom products and also for simple product
        variant.
        @author: Keyur Kanani
        :param order_line: sale.order.line() record.
        :param customers_location: stock.location()
        :param move_vals: stock move vals
        :return:
        """

        module_obj = self.env['ir.module.module']
        mrp_module = module_obj.sudo().search([('name', '=', 'mrp'), ('state', '=', 'installed')])
        if mrp_module:
            bom_lines = self.amz_shipment_get_set_product_ept(order_line.product_id)
            if bom_lines:
                for bom_line in bom_lines:
                    stock_move_vals = self.prepare_stock_move_vals(order_line, customers_location,
                                                                   move_vals)
                    stock_move_vals.update({'product_id': bom_line[0].product_id.id,
                                            'bom_line_id': bom_line[0].id,
                                            'product_uom_qty': bom_line[1].get(
                                                'qty', 0.0) * order_line.product_uom_qty})
                    self.process_shipment_report_stock_move_ept(stock_move_vals)
            else:
                stock_move_vals = self.prepare_stock_move_vals(order_line, customers_location,
                                                               move_vals)
                self.process_shipment_report_stock_move_ept(stock_move_vals)
        else:
            stock_move_vals = self.prepare_stock_move_vals(order_line, customers_location,
                                                           move_vals)
            self.process_shipment_report_stock_move_ept(stock_move_vals)
        return True

    def process_shipment_report_stock_move_ept(self, stock_move_vals):
        stock_move_obj = self.env[STOCK_MOVE]
        stock_move = stock_move_obj.create(stock_move_vals)
        stock_move._action_assign()
        stock_move._set_quantity_done(stock_move.product_uom_qty)
        return True

    def amz_shipment_get_set_product_ept(self, product):
        """
        Find BOM for phantom type only if Bill of Material type is Make to Order
        then for shipment report there are no logic to create Manufacturer Order.
        Author: Keyur Kanani
        :param product:
        :return:
        """
        try:
            bom_obj = self.env['mrp.bom']
            bom_point_dict = bom_obj.sudo()._bom_find(products=product, company_id=self.company_id.id,
                                                      bom_type='phantom')
            bom_point = bom_point_dict[product]
            from_uom = product.uom_id
            to_uom = bom_point.product_uom_id
            factor = from_uom._compute_quantity(1, to_uom) / bom_point.product_qty
            bom, lines = bom_point.explode(product, factor, picking_type=bom_point.picking_type_id)
            return lines
        except Exception:
            return {}

    def prepare_stock_move_vals(self, order_line, customers_location, move_vals):
        """
        Prepare stock move data for create stock move while validating sale order
        @author: Keyur kanani
        :param order_line:
        :param customers_location:
        :param move_vals:
        :return:
        """
        return {
            'name': _('Amazon move : %s') % order_line.order_id.name,
            'date': order_line.order_id.date_order,
            'company_id': self.company_id.id,
            'product_id': order_line.product_id.id,
            'product_uom_qty': move_vals.get('product_uom_qty',
                                             False) or order_line.product_uom_qty,
            'product_uom': order_line.product_uom.id,
            'location_id': order_line.order_id.warehouse_id.lot_stock_id.id,
            'location_dest_id': customers_location.id,
            'state': 'confirmed',
            'sale_line_id': order_line.id,
            'seller_id': self.seller_id.id,
            'amazon_shipment_id': move_vals.get('amazon_shipment_id', False),
            'amazon_instance_id': move_vals.get('amazon_instance_id', False),
            'amazon_order_reference': move_vals.get('amazon_order_reference', ''),
            'amazon_order_item_id': move_vals.get('amazon_order_item_id', False),
            'amazon_shipment_item_id': move_vals.get('amazon_shipment_item_id', False),
            'tracking_number': move_vals.get('tracking_number', ''),
            'fulfillment_center_id': move_vals.get('fulfillment_center_id', False),
            'amz_shipment_report_id': move_vals.get('amz_shipment_report_id', False)
        }

    @staticmethod
    def prepare_ship_partner_vals(row, instance):
        """
        Prepare Shipment Partner values
        @author: Keyur kanani
        :param row: {}
        :param instance: amazon.instance.ept()
        :return: {}
        """
        partner_vals = {
            'city': row.get('ship-city', False),
            'phone': row.get('ship-phone-number', False),
            'email': row.get('buyer-email', False),
            'zip': row.get('ship-postal-code', False),
            'lang': instance.lang_id and instance.lang_id.code,
            'company_id': instance.company_id.id,
            'is_amz_customer': True,
        }
        if instance.amazon_property_account_payable_id:
            partner_vals.update( \
                {'property_account_payable_id': instance.amazon_property_account_payable_id.id})
        if instance.amazon_property_account_receivable_id:
            partner_vals.update({
                'property_account_receivable_id': instance.amazon_property_account_receivable_id.id})
        return partner_vals

    def search_or_create_amz_partner(self, row, instance, ship_vals, partner_dict):
        res_partner_obj = self.env[RES_PARTNER]
        buyer_name = f"Amazon-{row.get('amazon-order-id')}"

        partner = res_partner_obj.with_context(is_amazon_partner=True).search(
            [('email', '=', row.get('buyer-email', '')),('state_id', '=', ship_vals.get('state_id')),
             ('country_id', '=', ship_vals.get('country_id')), ('city', '=', ship_vals.get('city')),
             ('zip', '=', ship_vals.get('zip')), '|', ('company_id', '=', False),
             ('company_id', '=', instance.company_id.id)], limit=1)
        if not partner:
            partnervals = {'name': buyer_name, 'type': 'delivery', **ship_vals}
            partner = res_partner_obj.create(partnervals)
            partner_dict.update({'invoice_partner': partner.id})
            delivery_partner = partner
        elif partner and row.get('amazon-order-id') not in partner.name.replace('Amazon-', '').split(','):
            partner.write({'name': f"{partner.name},{row.get('amazon-order-id')}"})
            delivery_partner = partner
        else:
            delivery_partner = partner
        return partner_dict, delivery_partner

    def search_or_create_partner(self, row, instance, partner_dict, country_dict, state_dict):
        """
        Search existing partner from order lines, if not exist then create New partner and
        if shipping partner is different from invoice partner then create new partner for shipment
        @author: Keyur Kanani
        :param row: {}
        :param instance:amazon.instance.ept()
        :param partner_dict: {}
        :return: {}
        """
        res_partner_obj = self.env[RES_PARTNER]
        buyers_name = f"Amazon-{row.get('amazon-order-id')}"
        ship_country = country_dict.get(row.get('ship-country', ''))
        if not ship_country:
            ship_country = self.env['res.country'].search_country_by_code_or_name(row.get('ship-country', ''))
            country_dict.update({row.get('ship-country', ''): ship_country})
        ship_vals = self.prepare_ship_partner_vals(row, instance)
        ship_state = state_dict.get(row.get('ship-state', ''), False)
        if not ship_state and ship_country and row.get('ship-state', '') != '--':
            ship_state = res_partner_obj.create_or_update_state_ept(ship_country.code,
                                                               row.get('ship-state', ''),
                                                               ship_vals.get('zip', ''), ship_country)
            state_dict.update({row.get('ship-state', ''): ship_state})
        ship_vals.update({'state_id': ship_state and ship_state.id or False,
                          'country_id': ship_country and ship_country.id or False})

        partner_dict, partner = self.search_or_create_amz_partner(row, instance, ship_vals, partner_dict)
        return {'invoice_partner': partner.id, 'shipping_partner': partner.id}, country_dict, state_dict

    def get_warehouse(self, fulfillment_center_id, instance):
        """
        Get Amazon fulfillment center and FBA warehouse id from current instance
        @author: Keyur Kanani
        :param fulfillment_center_id:
        :param instance: amazon.instance.ept()
        :return: fulfillment_center, warehouse
        """
        fulfillment_center_obj = self.env['amazon.fulfillment.center']
        fulfillment_center = fulfillment_center_obj.search( \
            [('center_code', '=', fulfillment_center_id),
             ('seller_id', '=', instance.seller_id.id)], limit=1)
        if self._context.get('is_fulfillment_center', False) and not fulfillment_center:
            warehouse = False
        else:
            warehouse = fulfillment_center and fulfillment_center.warehouse_id or \
                        instance.fba_warehouse_id or instance.warehouse_id or False
        return fulfillment_center, warehouse

    @staticmethod
    def prepare_amazon_sale_order_line_values(row, order_details_dict_list):
        """
        Prepare Sale Order lines vals for amazon orders
        @author: Keyur Kanani
        :param row:{}
        :return:{}
        """

        if row.get('merchant-order-id', False):
            if order_details_dict_list.get(row.get('merchant-order-id', False)):
                order_details_dict_list.get(row.get('merchant-order-id', False)).append(row)
            else:
                order_details_dict_list.update({row.get('merchant-order-id', False): [row]})
        else:
            if order_details_dict_list.get(row.get('amazon-order-id', False)):
                order_details_dict_list.get(row.get('amazon-order-id', False)).append(row)
            else:
                order_details_dict_list.update({row.get('amazon-order-id', False): [row]})
        return order_details_dict_list

    def check_odoo_proudct_for_import_order(self, row, instance, order_ref, odoo_product, seller_sku, job):
        odoo_product_obj = self.env['product.product']
        common_log_line_obj = self.env[COMMON_LOG_LINES_EPT]
        model_id = self.env[IR_MODEL]._get(AMZ_SHIPPING_REPORT_REQUEST_HISTORY).id
        skip_order = False
        if odoo_product:
            message = 'Odoo Product is already exists. System have created new Amazon ' \
                      'Product %s for %s instance' % (seller_sku, instance.name)
            common_log_line_obj.amazon_create_product_log_line(message, model_id,
                                                               odoo_product, seller_sku, 'FBA', job)
        elif not instance.seller_id.create_new_product:
            skip_order = True
            message = 'Skipped Amazon order %s because of Product %s not found for %s instance' % (
                order_ref, seller_sku, instance.name)
            common_log_line_obj.amazon_create_product_log_line(message, model_id,
                                                               False, seller_sku, 'FBA', job, mismatch=True)
        else:
            # #Create Odoo Product
            erp_prod_vals = {
                'name': row.get('product-name', ''),
                'default_code': seller_sku,
                'type': 'product',
                'purchase_ok': True,
                'sale_ok': True,
            }
            odoo_product = odoo_product_obj.create(erp_prod_vals)
            message = 'System have created new Odoo Product %s for %s instance' % (
                seller_sku, instance.name)
            common_log_line_obj.amazon_create_product_log_line(message, model_id,
                                                               odoo_product, seller_sku, 'FBA', job)
        return skip_order, odoo_product

    def prepare_amazon_products(self, lines, product_dict, order_ref, job):
        """
        Prepare Amazon Product values
        @author: Keyur Kanani
        :param lines: order lines
        :param product_dict: {}
        :param job: log record.
        :return: {boolean, product{}}

        If odoo product founds and amazon product not found then no need to
        check anything and create new amazon product and create log for that
        , if odoo product not found then go to check configuration which
        action has to be taken for that.
        There are following situations managed by code.
        In any situation log that event and action.

        1). Amazon product and odoo product not found
            => Check seller configuration if allow to create new product
            then create product.
            => Enter log details with action.
        2). Amazon product not found but odoo product is there.
            => Created amazon product with log and action.
        """

        amazon_product_obj = self.env['amazon.product.ept']
        amz_instance_obj = self.env[AMZ_INSTANCE_EPT]
        skip_order = False
        for row in lines:
            seller_sku = row.get('sku', '')
            instance = amz_instance_obj.browse(row.get('instance_id', False))
            odoo_product = product_dict.get((seller_sku, instance.id))
            if odoo_product:
                continue

            amazon_product = amazon_product_obj.search_amazon_product(instance.id, seller_sku, 'FBA')
            if not amazon_product:
                odoo_product = amazon_product_obj.search_product(seller_sku)
                skip_order, odoo_product = self.check_odoo_proudct_for_import_order(row, instance, order_ref,
                                                                                    odoo_product, seller_sku, job)
                if not skip_order:
                    sku = seller_sku or (odoo_product and odoo_product[0].default_code) or False
                    # #Prepare Product Values
                    prod_vals = self.prepare_amazon_prod_vals(instance, row, sku, odoo_product)
                    # #Create Amazon Product
                    amazon_product_obj.create(prod_vals)
                if odoo_product:
                    product_dict.update({(seller_sku, instance.id): odoo_product})
            else:
                product_dict.update({(seller_sku, instance.id): amazon_product.product_id})
        return skip_order, product_dict

    @staticmethod
    def prepare_amazon_prod_vals(instance, order_line, sku, odoo_product):
        """
        Prepare Amazon Product Values
        @author: Keyur Kanani
        :param instance: amazon.instance.ept()
        :param order_line: {}
        :param sku: string
        :param odoo_product: product.product()
        :return: {}
        """
        prod_vals = {}
        prod_vals.update({
            'name': order_line.get('product-name', False),
            'instance_id': instance.id,
            'product_asin': order_line.get('ASIN', False),
            'seller_sku': sku,
            'product_id': odoo_product and odoo_product.id or False,
            'exported_to_amazon': True, 'fulfillment_by': 'FBA'
        })
        return prod_vals

    def request_shipment_report_by_software(self, seller_id, start_date, end_date):
        """
        This method will create and request for shipment report
        """
        report_type = ReportType.GET_AMAZON_FULFILLED_SHIPMENTS_DATA
        if not self.search([('start_date', '=', start_date),
                            ('end_date', '=', end_date),
                            ('seller_id', '=', seller_id),
                            ('report_type', '=', report_type)]):
            shipment_report = self.create({'report_type': report_type,
                                           'seller_id': seller_id,
                                           'state': 'draft',
                                           'start_date': start_date,
                                           'end_date': end_date,
                                           'requested_date': time.strftime(DATE_YMDHMS)})
            shipment_report.with_context({'is_auto_process': True,
                                          'emipro_api': 'create_report_sp_api'}).request_report()

    def search_or_create_amz_shipment_report(self, seller_id, report):
        """
        Purpose: Serach if shipment report is already imported in odoo with the same report id
        then do not create duplicate report, otherwise create new one.
        :param seller_id: amazon.seller.ept()
        :param report: response data from amazon
        :return:
        """
        report_id = report.get('reportId', '')
        report_document_id = report.get('reportDocumentId', '')
        report_type = ReportType.GET_AMAZON_FULFILLED_SHIPMENTS_DATA
        if not self.search([('report_id', '=', report_id),
                            ('report_document_id', '=', report_document_id),
                            ('report_type', '=', report_type)]):
            start = parser.parse(str(report.get('dataStartTime', ''))).astimezone(utc).strftime(DATE_YMDHMS)
            end = parser.parse(str(report.get('dataEndTime', ''))).astimezone(utc).strftime(DATE_YMDHMS)
            shipment_report = self.create({'seller_id': seller_id,
                                           'state': report.get('processingStatus', ''),
                                           'start_date': datetime.strptime(start, DATE_YMDHMS),
                                           'end_date': datetime.strptime(end, DATE_YMDHMS),
                                           'report_type': report.get('reportType', ''),
                                           'report_id': report.get('reportId', ''),
                                           'report_document_id': report.get('reportDocumentId', ''),
                                           'requested_date': time.strftime(DATE_YMDHMS)
                                           })
            return shipment_report

    @api.model
    def auto_import_shipment_report(self, args):
        """
        auto import shipment report
        """
        seller_id = args.get('seller_id', False)
        if seller_id:
            seller = self.env[AMZ_SELLER_EPT].browse(int(seller_id))
            if seller.shipping_report_last_sync_on:
                start_date = seller.shipping_report_last_sync_on - timedelta(hours=10)
            else:
                today = datetime.now()
                start_date = today - timedelta(days=30)
            start_date = start_date + timedelta(days=seller.shipping_report_days * -1 or -3)
            end_date = datetime.now()

            if not seller.is_another_soft_create_fba_shipment:
                self.request_shipment_report_by_software(seller_id, start_date, end_date)
            else:
                self.process_and_create_amz_shipment_report(seller, start_date, end_date)
            seller.write({'shipping_report_last_sync_on': end_date.strftime(DATE_YMDHMS)})
        return True

    def process_and_create_amz_shipment_report(self, seller, start_date, end_date):
        """
        Define method which help to process and create shipment report.
        :param : seller : amazon.seller.ept() object
        :param : start_date : report request start date
        :param : end_date : report request end date
        """
        date_start = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_end = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        list_of_wrapper = self.get_reports_from_other_softwares(seller, date_start, date_end)
        list_of_wrapper = list_of_wrapper.get('reports', {}) if list_of_wrapper else []
        shipment_reports = []
        for report in list_of_wrapper:
            shipment_report = self.search_or_create_amz_shipment_report(seller.id, report)
            if shipment_report:
                shipment_reports.append(shipment_report.id)
        return shipment_reports

    def auto_process_shipment_report(self, args={}):
        """
        Auto process shipment report
        """
        seller_id = args.get('seller_id', False)
        if seller_id:
            seller = self.env[AMZ_SELLER_EPT].browse(seller_id)
            ship_reports = self.search([('seller_id', '=', seller.id),
                                        ('state', 'in', ['_SUBMITTED_', '_IN_PROGRESS_', '_DONE_',
                                                         'SUBMITTED', 'IN_PROGRESS', 'DONE',
                                                         'IN_QUEUE', 'partially_processed'])])
            for report in ship_reports:
                report_type = '' if self.report_type == "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL" else \
                    'shipment_report_spapi'
                if report.state not in ['_DONE_', 'DONE', 'partially_processed']:
                    report.with_context(
                        is_auto_process=True, amz_report_type=report_type).get_report_request_list()
                if report.report_id and report.state in ['_DONE_', 'DONE', 'partially_processed'] \
                        and not report.attachment_id:
                    report.with_context(is_auto_process=True, amz_report_type=report_type).get_report()
                if report.attachment_id:
                    report.with_context(is_auto_process=True).process_shipment_file()
                self._cr.commit()
        return True

    def get_reports_from_other_softwares(self, seller, start_date, end_date):
        """
        will request for the amazon shipment report and return response
        """
        kwargs = self.prepare_amazon_request_report_kwargs(seller)
        report_type = ReportType.GET_AMAZON_FULFILLED_SHIPMENTS_DATA
        kwargs.update({'emipro_api': 'get_reports_sp_api',
                       'report_type': [report_type],
                       'start_date': start_date,
                       'end_date': end_date})
        response = iap_tools.iap_jsonrpc(DEFAULT_ENDPOINT, params=kwargs, timeout=1000)
        if not response.get('error', False):
            list_of_wrapper = response.get('result', [])
        else:
            raise UserError(_(response.get('error', {})))
        return list_of_wrapper

    def get_missing_fulfillment_center(self, attachment_id):
        """
        All Fulfillment Center from attachment file and find in ERP
        If Fulfillment Center doesn't exist in ERP then it will return in list
        @:param - attachment_id - shipping report attachment
        @:return - unavailable_fulfillment_center - return missing fulfillment center from ERP
        @author: Deval Jagad (09/01/2020)
        """
        fulfillment_center_obj = self.env['amazon.fulfillment.center']
        unavailable_fulfillment_center = []
        log = self.with_context(is_missing_fulfillment_center=True).amz_search_or_create_logs_ept('')
        if self.report_type == "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL":
            imp_file = StringIO(base64.decodebytes(attachment_id.datas).decode())
        else:
            imp_file = self.decode_amazon_encrypted_attachments_data(attachment_id, log)
        reader = csv.DictReader(imp_file, delimiter='\t')
        fulfillment_centers = [row.get('fulfillment-center-id') for row in reader]
        fulfillment_center_list = fulfillment_centers and list(set(fulfillment_centers))
        seller_id = self.seller_id.id

        for fulfillment_center in fulfillment_center_list:
            amz_fulfillment_center_id = fulfillment_center_obj.search([('center_code', '=', fulfillment_center),
                                                                       ('seller_id', '=', seller_id)])
            if not amz_fulfillment_center_id:
                unavailable_fulfillment_center.append(fulfillment_center)

        if log and not log.log_lines:
            log.unlink()

        return unavailable_fulfillment_center

    def configure_missing_fulfillment_center(self):
        """
        Open wizard with load missing fulfillment center from ERP
        @author: Deval Jagad (07/01/2020)
        """
        view = self.env.ref('amazon_ept.view_configure_shipment_report_fulfillment_center_ept')
        context = dict(self._context)
        self.check_fba_warehouse_partner_and_country_configured()
        country_ids = self.seller_id.amz_warehouse_ids.mapped('partner_id').mapped('country_id')
        context.update({'shipment_report_id': self.id, 'country_ids': country_ids.ids})

        return {
            'name': _('Amazon Shipment Report - Configure Missing Fulfillment Center'),
            'type': IR_ACTION_ACT_WINDOW,
            'view_type': 'form',
            'view_mode': 'form',
            'res_model': 'shipment.report.configure.fulfillment.center.ept',
            'views': [(view.id, 'form')],
            'view_id': view.id,
            'target': 'new',
            'context': context,
        }

    def check_fba_warehouse_partner_and_country_configured(self):
        """
        Define method for check partner and country set in Amazon FBA warehouses.
        :return:
        @author: Kishan Sorani on date 16-Nov-2021
        """
        if not self.seller_id.amz_warehouse_ids:
            UserError(_("Please Set Amazon FBA Warehouses in Seller [%s]" % (self.seller_id.name)))
        missing_partners = self.seller_id.amz_warehouse_ids.filtered(lambda warehouse: not warehouse.partner_id)
        if missing_partners:
            raise UserError(_("Please Configure Partner in following Amazon FBA Warehouses: %s" %
                              (missing_partners.mapped('name'))))
        missing_warehouses = self.seller_id.amz_warehouse_ids.filtered(
            lambda warehouse: not warehouse.partner_id.country_id)
        if missing_warehouses:
            missing_country_instance = self.seller_id.instance_ids.filtered(
                lambda wh: wh.fba_warehouse_id in missing_warehouses and wh.country_id.code not in ['NL', 'SE'])
            message = ""
            for inst in missing_country_instance:
                message += "Country [%s] must be required in Amazon FBA Warehouse [%s] in Partner [%s] " % (
                    inst.country_id.code, inst.fba_warehouse_id.name, inst.fba_warehouse_id.partner_id.name)
            raise UserError(_(message))

    def decode_amazon_encrypted_attachments_data(self, attachment_id, job):
        """
        This method will decode the amazon encrypted data
        """
        dbuuid = self.env['ir.config_parameter'].sudo().get_param('database.uuid')
        kwargs = {'dbuuid': dbuuid, 'report_id': self.report_id, 'datas': attachment_id.datas.decode(),
                  'report_document_id': self.report_document_id, 'amz_report_type': 'shipment_report_spapi',
                  'merchant_id': self.seller_id.merchant_id}
        imp_file = []
        response = iap_tools.iap_jsonrpc(DECODE_ENDPOINT, params=kwargs, timeout=1000)
        if response.get('result', False):
            try:
                imp_file = StringIO(base64.b64decode(response.get('result', {})).decode())
            except Exception:
                imp_file = StringIO(base64.b64decode(response.get('result', {})).decode('ISO-8859-1'))
        elif not self._context.get('is_auto_process', False):
            raise UserError(_(response.get('error', '')))
        else:
            job.log_lines.create({'message': 'Error found in Decryption of Data %s' % response.get('error', ''),
                                  'mismatch_details': True})
        return imp_file
