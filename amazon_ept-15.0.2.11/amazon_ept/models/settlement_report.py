# -*- coding: utf-8 -*-
# Part of Odoo. See LICENSE file for full copyright and licensing details.

"""
Added class to process amazon settlement report.
"""

import base64
import csv
import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from io import StringIO
from odoo import models, fields, api, _
from odoo.exceptions import UserError

AMAZON_SELLER_EPT = 'amazon.seller.ept'
ACCOUNT_MOVE = 'account.move'
ACCOUNT_MOVE_LINE = 'account.move.line'
ACCOUNT_BANK_STATEMENT = 'account.bank.statement'
ACCOUNT_BANK_STATEMENT_LINE = 'account.bank.statement.line'
ACCOUNT_TRANSACTION_LINE_EPT = 'amazon.transaction.line.ept'
SALE_ORDER = 'sale.order'
ENDING_BALANCE_DESC = 'Ending Balance Description'
DATE_YMDHMS = "%Y-%m-%d %H:%M:%S"
DATE_YMD = '%Y-%m-%d'

_logger = logging.getLogger(__name__)


class SettlementReportEpt(models.Model):
    _name = "settlement.report.ept"
    _order = 'id desc'
    _inherit = ['mail.thread', 'amazon.reports']
    _description = "Settlement Report"

    @api.depends('seller_id')
    def _compute_settlement_company(self):
        """
        Migration done by twinakalc on 28 sep, 2020,
        This method will set the company into the settlement report.
        """
        for record in self:
            company_id = record.seller_id.company_id.id if record.seller_id else False
            if not company_id:
                company_id = self.env.company.id
            record.company_id = company_id

    def _compute_invoices(self):
        """
        Migration done by twinakal on 28 sep, 2020,
        This method will count the number of reimbursement invoices.
        """
        self.invoice_count = len(self.reimbursement_invoice_ids.ids)

    def _compute_is_fee(self):
        """
        Migration done by twinakalc on 28 sep, 2020,
        This method will set is_fees to true if any amazon
        fees is remain to configure
        """
        for record in self:
            is_fee = False
            state = record.state
            if state in ['imported', 'partially_processed', '_DONE_', 'DONE']:
                amazon_code_list = record.statement_id.line_ids.filtered( \
                    lambda x: x.amazon_code and not x.is_reconciled).mapped( \
                    'amazon_code')
                statement_amazon_code = amazon_code_list and list(set(amazon_code_list))
                transaction_amazon_code_list = record.seller_id.transaction_line_ids.filtered( \
                    lambda x: x.amazon_code).mapped('amazon_code')
                missing_account_id_list = record.seller_id.transaction_line_ids.filtered(
                    lambda l: not l.account_id).mapped('amazon_code')
                transaction_amazon_code = transaction_amazon_code_list and list(
                    set(transaction_amazon_code_list))
                unavailable_amazon_code = [code for code in statement_amazon_code if
                                           code not in transaction_amazon_code or
                                           code in missing_account_id_list]

                if unavailable_amazon_code:
                    is_fee = True
            record.is_fees = is_fee

    name = fields.Char(size=256, default='XML Settlement Report')
    attachment_id = fields.Many2one('ir.attachment', string="Attachment")
    statement_id = fields.Many2one(ACCOUNT_BANK_STATEMENT, string="Bank Statement")
    seller_id = fields.Many2one(AMAZON_SELLER_EPT, string='Seller', copy=False)
    instance_id = fields.Many2one('amazon.instance.ept', string="Marketplace")
    currency_id = fields.Many2one('res.currency', string="Currency")
    user_id = fields.Many2one('res.users', string="Requested User")
    company_id = fields.Many2one('res.company', string="Company", copy=False,
                                 compute="_compute_settlement_company",
                                 store=True)
    start_date = fields.Date()
    end_date = fields.Date()
    already_processed_report_id = fields.Many2one("settlement.report.ept",
                                                  string="Already Processed Report")
    reimbursement_invoice_ids = fields.Many2many("account.move",
                                                 'amazon_reimbursement_invoice_rel', 'report_id',
                                                 'invoice_id', string="Reimbursement Invoice")
    invoice_count = fields.Integer(compute='_compute_invoices', readonly=True)
    report_id = fields.Char(size=256, string='Report ID')
    report_type = fields.Char(size=256)
    report_request_id = fields.Char(size=256, string='Report Request ID')
    report_document_id = fields.Char(string='Report Document ID',
                                     help="Report Document id to recognise unique request document reference")
    requested_date = fields.Datetime(default=time.strftime(DATE_YMDHMS))
    state = fields.Selection([('draft', 'Draft'), ('_SUBMITTED_', 'SUBMITTED'),
                              ('_IN_PROGRESS_', 'IN_PROGRESS'), ('_CANCELLED_', 'CANCELLED'),
                              ('_DONE_', 'DONE'), ('IN_PROGRESS', 'IN_PROGRESS'),
                              ('FATAL', 'FATAL'), ('CANCELLED', 'CANCELLED'), ('DONE', 'DONE'),
                              ('IN_QUEUE', 'IN_QUEUE'), ('SUBMITTED', 'SUBMITTED'),
                              ('_DONE_NO_DATA_', 'DONE_NO_DATA'), ('processed', 'PROCESSED'),
                              ('imported', 'Imported'), ('duplicate', 'Duplicate'),
                              ('partially_processed', 'Partially Processed'), ('confirm', 'Validated')],
                             string='Report Status', default='draft')
    is_fees = fields.Boolean(string="Is Fee", compute='_compute_is_fee', store=False)
    all_lines_reconciled = fields.Boolean(string="Is All Lines Reconciled?",
                                          related="statement_id.all_lines_reconciled", store=False)

    def list_of_reimbursement_invoices(self):
        """
        Migration done by twinkalc on 28 sep, 2020,
        This method will return the reimbursement invoice list
        """
        invoices = self.reimbursement_invoice_ids
        action = self.env.ref('account.action_move_out_invoice_type').read()[0]
        if len(invoices) > 1:
            action['domain'] = [('id', 'in', invoices.ids)]
        elif len(invoices) == 1:
            form_view = [(self.env.ref('account.view_move_form').id, 'form')]
            if 'views' in action:
                action['views'] = form_view + [(state, view) for state, view in action['views'] if
                                               view != 'form']
            else:
                action['views'] = form_view
            action['res_id'] = invoices.id
        else:
            action = {'type': 'ir.actions.act_window_close'}
        return action

    def unlink(self):
        """
        Added by udit
        Migration done by twinkalc on 28 sep, 2020,
        This method will not delete settlement reports which are in 'processed'
        state.
        """
        for report in self:
            if report.state == 'processed':
                raise UserError(_('You cannot delete processed report.'))
        return super(SettlementReportEpt, self).unlink()

    def validate_statement(self):
        """
        Migration done by twinkalc on 28 sep, 2020,
        This method will validate the reconciled statement and update
        the settlement report state to validated.
        """
        self.statement_id.button_validate_or_action()
        if self.state != 'confirm':
            self.write({'state': 'confirm'})
        return True

    @staticmethod
    def prepare_settlement_order_name_list(order_names):
        """
        This method will prepare the amazon order list
        """
        order_name_list = []
        for order_name in order_names:
            order_name_list.append(order_name.split("/")[-1])
        return order_name_list

    @staticmethod
    def prepare_settlement_refund_name_list(refund_names):
        """
        This method will prepare the amazon refund list
        """
        refund_name_list = []
        for refund_name in refund_names:
            if '/' in refund_name:
                refund_name_list.append(refund_name.split("/")[-1])
            else:
                refund_name_list.append(refund_name.replace('Refund_', ''))
        return refund_name_list

    @api.model
    def remaining_order_refund_lines(self):
        """This function is used to get remaining order lines for reconciliation
            @author: Dimpal added on 15/oct/2019

        Migration done by twinkalc on 28 sep, 2020,
        Changes : filter the statement lines to process for reconciliation
        :Updated by : Kishan Sorani on date 22-Jul-2021
        :MOD : consider ItemPrice and Promotion lines of order for bank statement
               line reconciliation for refund invoice
        Case 1 : When sale order is imported at that time sale order is in quotation state,
        so after Importing Settlement report it will receive payment line for that invoice,
        system will not reconcile invoice of those orders, because pending quotation. """

        sale_order_obj = self.env[SALE_ORDER]
        amazon_product_obj = self.env['amazon.product.ept']
        partner_obj = self.env['res.partner']

        order_statement_lines = self.statement_id.line_ids.filtered(
            lambda x: not x.is_refund_line and not x.sale_order_id and not x.amazon_code and not x.is_reconciled)
        order_names = order_statement_lines.mapped('payment_ref')
        order_name_list = self.prepare_settlement_order_name_list(order_names)

        refund_lines = self.statement_id.line_ids.filtered(
            lambda x: x.is_refund_line and not x.amazon_code and not x.is_reconciled and not x.refund_invoice_id)
        refund_names = refund_lines.mapped('payment_ref')
        refund_name_list = self.prepare_settlement_refund_name_list(refund_names)
        if order_names and refund_names:
            imp_file = StringIO(base64.b64decode(self.attachment_id.datas).decode())
            content = imp_file.read()
            delimiter = ('\t', csv.Sniffer().sniff(content.splitlines()[0]).delimiter)[bool(content)]
            settlement_reader = csv.DictReader(content.splitlines(), delimiter=delimiter)
            order_dict = {}
            product_dict = {}
            create_or_update_refund_dict = {}
            for row in settlement_reader:
                if row.get('amount-description').__contains__('MarketplaceFacilitator') or row.get(
                        'amount-type') == 'ItemFees':
                    continue
                order_ref = row.get('order-id', '')
                adjustment_id = row.get('adjustment-id', '')
                if order_ref not in order_name_list and order_ref not in refund_name_list:
                    continue
                shipment_id = row.get('shipment-id', '')
                order_item_code = row.get('order-item-code', '').lstrip('0')
                posted_date = row.get('posted-date', '')
                fulfillment_by = row.get('fulfillment-id', '')
                transaction_type = row.get('transaction-type', '')
                posted_date = self.get_amz_settlement_posted_date(posted_date)
                order_ids = order_dict.get((order_ref, shipment_id, order_item_code))
                if not order_ids:
                    amz_order = self.get_settlement_report_amazon_order_ept(row)
                    order_ids = tuple(amz_order.ids)
                    order_dict.update({(order_ref, shipment_id, order_item_code): order_ids})
                else:
                    amz_order = sale_order_obj.browse(order_ids)
                if not amz_order:
                    continue
                partner = partner_obj.with_context(is_amazon_partner=True)._find_accounting_partner( \
                    amz_order.mapped('partner_id'))
                if order_ref in order_name_list and amz_order and transaction_type == 'Order':
                    order_line_name = self.statement_id.settlement_ref + '/' + shipment_id + '/' + order_ref
                    order_statement_lines.filtered(
                        lambda l, order_line_name=order_line_name: l.payment_ref == order_line_name and not
                        l.sale_order_id).write(\
                        {'sale_order_id': amz_order.ids[0], 'partner_id': partner.id if partner else False})
                elif order_ref in refund_name_list and amz_order and transaction_type == 'Refund':
                    product_id = product_dict.get(row.get('sku', ''))
                    if not product_id:
                        amazon_product = amazon_product_obj.search([('seller_sku', '=', row.get('sku', '')),
                                                                    ('instance_id', '=', self.instance_id.id)], limit=1)
                        product_id = amazon_product.product_id.id
                        product_dict.update({row.get('sku', ''): amazon_product.product_id.id})

                    key = (order_ref, order_ids, posted_date, fulfillment_by, partner.id, adjustment_id)
                    create_or_update_refund_dict = self.get_settlement_refund_dict_ept(row, key, product_id,
                                                                                       create_or_update_refund_dict)
                    adjustment_id = (adjustment_id + '/') if adjustment_id else ''
                    ref_name = 'Refund_' + adjustment_id + order_ref
                    refund_lines.filtered(
                        lambda l, ref_name=ref_name: l.payment_ref == ref_name and not l.sale_order_id).write( \
                        {'sale_order_id': amz_order.ids[0], 'partner_id': partner.id if partner else False})

            if create_or_update_refund_dict:
                self.create_refund_invoices(create_or_update_refund_dict, self.statement_id)

        return True

    def reconcile_reimbursement_lines(self, seller, bank_statement, statement_lines, \
                                      fees_transaction_dict):
        """
        Migration done by twinkalc on 28 sep, 2020,
        This method will reconcile the reimbursement lines.
        """
        transaction_obj = self.env[ACCOUNT_TRANSACTION_LINE_EPT]
        bank_statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]

        reimbursement_invoice_ids = self.reimbursement_invoice_ids
        statement_lines = bank_statement_line_obj.browse(statement_lines)

        for line in statement_lines:
            trans_line_id = fees_transaction_dict.get(line.amazon_code)
            trans_line = transaction_obj.browse(trans_line_id)
            date_posted = line.date
            invoice_type = 'out_refund' if line.amount < 0.00 else 'out_invoice'
            reimbursement_invoice_amount = abs(line.amount)
            reimbursement_invoice = reimbursement_invoice_ids.filtered( \
                lambda l, invoice_type=invoice_type: l.state in ['draft', 'posted'] and not l.payment_state in ['paid',
                                                                                                                'in_payment'] and l.move_type == invoice_type)
            if len(reimbursement_invoice) > 1:
                reimbursement_invoice = reimbursement_invoice.filtered(
                    lambda l, reimbursement_invoice_amount=reimbursement_invoice_amount: round(
                        l.amount_total, 10) == round(reimbursement_invoice_amount, 10))
                reimbursement_invoice = reimbursement_invoice[0] if reimbursement_invoice else False
            if not reimbursement_invoice:
                reimbursement_invoice = self.create_amazon_reimbursement_invoice(bank_statement, seller, date_posted,
                                                                                 invoice_type)
                self.create_amazon_reimbursement_invoice_line(seller, reimbursement_invoice, line.name,
                                                              reimbursement_invoice_amount, trans_line)
                self.write({'reimbursement_invoice_ids': [(4, reimbursement_invoice.id)]})
            self.reconcile_reimbursement_invoice(reimbursement_invoice, line, bank_statement)
        return True

    def get_amz_paid_move_line_total_amount(self, move_line_total_amount, paid_invoices, currency_ids):
        """
        Added method to get move line total amount of paid invoices
        """
        bank_statement = self._context.get('statement_id', False)
        statement_line = self._context.get('statement_line', False)
        payment_id = paid_invoices.line_ids.matched_credit_ids.credit_move_id.payment_id
        paid_move_lines = payment_id.invoice_line_ids.filtered(lambda x: x.debit != 0.0)
        for moveline in paid_move_lines:
            amount = moveline.debit - moveline.credit
            amount_currency = 0.0
            if moveline.amount_currency:
                currency, amount_currency = self.convert_move_amount_currency(
                    bank_statement, moveline, amount, statement_line.date)
                if currency:
                    currency_ids.append(currency)

            if amount_currency:
                amount = amount_currency

            move_line_total_amount += amount
        return move_line_total_amount, currency_ids

    def get_amz_unpaid_move_line_total_amount(self, move_line_total_amount, unpaid_invoices, currency_ids, mv_line_dicts):
        """
        Added method to get move line total amount of unpaid invoices
        """
        bank_statement = self._context.get('statement_id', False)
        statement_line = self._context.get('statement_line', False)
        move_lines = unpaid_invoices.line_ids.filtered(
            lambda l: l.account_id.user_type_id.type == 'receivable' and not l.reconciled)
        for moveline in move_lines:
            amount = moveline.debit - moveline.credit
            amount_currency = 0.0
            if moveline.amount_currency:
                currency, amount_currency = self.convert_move_amount_currency(
                    bank_statement, moveline, amount, statement_line.date)
                if currency:
                    currency_ids.append(currency)

            if amount_currency:
                amount = amount_currency
            mv_line_dicts.append({
                'name': moveline.move_id.name,
                'id': moveline.id,
                'balance': -amount,
                'currency_id': moveline.currency_id.id,
            })
            move_line_total_amount += amount

        return move_line_total_amount, currency_ids, mv_line_dicts

    def reconcile_orders(self, statement_lines):
        """This function is used to reconcile bank statement which is generated from settlement report
            @author: Dimpal added on 15/oct/2019

            Migration done by twinkalc on 28 sep, 2020,
            Updated changes related to reconcile the paid and unpaid invoices
            of orders
        """
        bank_statement = self.statement_id
        statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        for statement_line_id in statement_lines:
            statement_line = statement_line_obj.browse(statement_line_id)
            order = statement_line.sale_order_id
            try:
                self.check_or_create_invoice_if_not_exist(order)
            except Exception as ex:
                _logger.error(ex)
            invoices = order.invoice_ids.filtered(
                lambda record: record.move_type == 'out_invoice' and record.state in ['posted'])
            if not invoices:
                continue
            if len(invoices) > 1:
                reconcile_invoice = invoices.filtered(
                    lambda l, statement_line=statement_line: round(l.amount_total, 10) == round(statement_line.amount,
                                                                                                10))
                reconcile_invoice = reconcile_invoice.line_ids.filtered(
                    lambda l: l.account_id.user_type_id.type == 'receivable' and not l.reconciled).mapped('move_id')
                if reconcile_invoice:
                    invoices = reconcile_invoice[0]

            paid_invoices = invoices.filtered(lambda record: record.payment_state in ['in_payment'])
            if len(paid_invoices) > 1:
                paid_invoices = invoices.filtered(
                    lambda l, statement_line=statement_line: round(l.amount_total, 10) == round(statement_line.amount,
                                                                                                10))
                paid_invoices = paid_invoices and paid_invoices[0]

            unpaid_invoices = invoices.filtered(lambda record: record.payment_state == 'not_paid')
            mv_line_dicts = []
            move_line_total_amount = 0.0
            currency_ids = []
            paid_move_lines = []

            ctx = {'statement_id': bank_statement, 'statement_line': statement_line}
            if paid_invoices:
                move_line_total_amount, currency_ids = self.with_context(**ctx).get_amz_paid_move_line_total_amount(\
                    move_line_total_amount, paid_invoices, currency_ids)

            if unpaid_invoices:
                move_line_total_amount, currency_ids, mv_line_dicts = self.with_context(
                    **ctx).get_amz_unpaid_move_line_total_amount(\
                    move_line_total_amount, unpaid_invoices, currency_ids, mv_line_dicts)

            if round(statement_line.amount, 10) == round(move_line_total_amount, 10) and (
                    not statement_line.currency_id or statement_line.currency_id.id == bank_statement.currency_id.id):
                if currency_ids:
                    currency_ids = list(set(currency_ids))
                    if len(currency_ids) == 1:
                        statement_currency = statement_line.journal_id.currency_id and \
                                             statement_line.journal_id.currency_id.id or \
                                             statement_line.company_id.currency_id and \
                                             statement_line.company_id.currency_id.id
                        if not currency_ids[0] == statement_currency:
                            vals = {'currency_id': currency_ids[0], }
                            statement_line.write(vals)
                if mv_line_dicts:
                    statement_line.reconcile(lines_vals_list=mv_line_dicts)
                for payment_line in paid_move_lines:
                    statement_line.reconcile(([{'id': payment_line.id}]))

    def reconcile_refunds(self, statement_lines):
        """
        Migration done by twinkalc on 28 sep, 2020,
        Updated changes related to reconcile the paid and unpaid invoices
        of refund orders
        """
        bank_statement = self.statement_id
        statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]

        for statement_line_id in statement_lines:
            statement_line = statement_line_obj.browse(statement_line_id)
            paid_move_lines = []
            mv_line_dicts = []
            move_line_total_amount = 0.0
            currency_ids = []

            if statement_line.refund_invoice_id.payment_state in ['paid', 'in_payment']:
                payment_id = statement_line.refund_invoice_id.line_ids.matched_debit_ids.debit_move_id.payment_id
                paid_move_lines = payment_id.invoice_line_ids.filtered(lambda x: x.credit != 0.0)
                for moveline in paid_move_lines:
                    amount = moveline.debit - moveline.credit
                    amount_currency = 0.0
                    if moveline.amount_currency:
                        currency, amount_currency = self.convert_move_amount_currency(
                            bank_statement, moveline, amount, statement_line.date)
                        if currency:
                            currency_ids.append(currency)
                    if amount_currency:
                        amount = amount_currency
                    move_line_total_amount += amount
            else:
                unpaid_move_lines = statement_line.refund_invoice_id.line_ids.filtered(
                    lambda l: l.account_id.user_type_id.type == 'receivable' and not l.reconciled)
                for moveline in unpaid_move_lines:
                    amount = moveline.debit - moveline.credit
                    amount_currency = 0.0
                    if moveline.amount_currency:
                        currency, amount_currency = self.convert_move_amount_currency(
                            bank_statement, moveline, amount, statement_line.date)
                        if currency:
                            currency_ids.append(currency)
                    if amount_currency:
                        amount = amount_currency

                    mv_line_dicts.append({
                        'name': moveline.move_id.name,
                        'id': moveline.id,
                        'balance': -amount,
                        'currency_id': moveline.currency_id.id,
                    })
                    move_line_total_amount += amount
            if round(statement_line.amount, 10) == round(move_line_total_amount, 10) and (
                    not statement_line.currency_id or statement_line.currency_id.id == bank_statement.currency_id.id):
                if currency_ids:
                    currency_ids = list(set(currency_ids))
                    if len(currency_ids) == 1:
                        statement_currency = statement_line.journal_id.currency_id and \
                                             statement_line.journal_id.currency_id.id or \
                                             statement_line.company_id.currency_id and \
                                             statement_line.company_id.currency_id.id
                        if not currency_ids[0] == statement_currency:
                            statement_line.write({'currency_id': currency_ids[0]})

                if mv_line_dicts:
                    statement_line.reconcile(lines_vals_list=mv_line_dicts)
                for payment_line in paid_move_lines:
                    statement_line.reconcile(([{'id': payment_line.id}]))

        return True

    def search_and_reconile_ending_balance_line(self):
        """
        Ths method will search and recocile the ending balance line
        """
        statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        account_statement = self.statement_id
        if account_statement.state == 'open':
            account_statement.button_post()

        ending_balance_line = statement_line_obj.search([('is_ending_balance_entry', '=', True), \
                                                         ('statement_id', '=', account_statement.id)])

        if ending_balance_line and not ending_balance_line.is_reconciled:
            mv_dicts = {
                'name': self.instance_id.ending_balance_description or ENDING_BALANCE_DESC,
                'account_id': self.instance_id.ending_balance_account_id.id,
                'balance': -ending_balance_line.amount, }
            ending_balance_line.reconcile(lines_vals_list=[mv_dicts])
        return True

    def reconcile_amazon_statement_lines(self, rei_lines, fees_transaction_dict):
        """
        This method will find the amazon statement lines which needs to reconcile
        """
        for line in range(0, len(rei_lines), 10):
            lines = rei_lines[line:line + 10]
            self.reconcile_reimbursement_lines(self.seller_id, self.statement_id, lines, fees_transaction_dict)
            self._cr.commit()

        statement_lines = self.statement_id.line_ids.filtered(
            lambda x: not x.is_reconciled and not x.amazon_code and not x.is_refund_line and x.sale_order_id)
        for line in range(0, len(statement_lines), 10):
            lines = statement_lines[line:line + 10]
            self.reconcile_orders(lines.ids)
            self._cr.commit()

        statement_lines = self.statement_id.line_ids.filtered(
            lambda x: not x.is_reconciled and not x.amazon_code and x.is_refund_line and x.refund_invoice_id)
        for x in range(0, len(statement_lines), 10):
            lines = statement_lines[x:x + 10]
            self.reconcile_refunds(lines.ids)
            self._cr.commit()

        if not self.statement_id.line_ids.filtered(lambda x: not x.is_reconciled):
            self.write({'state': 'processed'})
        else:
            if self.state != 'partially_processed':
                self.write({'state': 'partially_processed'})
        return True

    def reconcile_other_transaction_line_with_taxes(self, tax_id, line_id, move_line_list, account_id,
                                                    analytic_account_id):
        """
        This method will prepare move line  list which transaction line contains taxes.
        Needs to reconcile the tax lines with the tax account
        """
        mv_line_data_list = []
        statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        line = statement_line_obj.browse(line_id)
        mv_line_dict = tax_id.json_friendly_compute_all(line.amount, line.currency_id.id)
        mv_line_tax_list = mv_line_dict.get('taxes', [])
        mv_line_tag_ids = mv_line_dict.get('base_tags', [])

        if mv_line_tax_list:
            base_amount = mv_line_tax_list[0].get('base')
            mv_line_data_list.append({'move_line': {'name': line.name, 'amount': base_amount, 'account_id': account_id,
                                                    'tax_tag_ids': mv_line_tag_ids,
                                                    'analytic_account_id': analytic_account_id}})
        for move_tax_data in mv_line_tax_list:
            vat_mv_line = line.name + ' ' + move_tax_data.get('name', 'VAT')
            vat_amount = move_tax_data.get('amount', 0.0)
            vat_account_id = move_tax_data.get('account_id', False)
            vt_line_tag_ids = move_tax_data.get('tag_ids', [])
            if not vat_account_id:
                mv_line_data_list = []
                break
            mv_line_data_list.append({'vat_line': {'name': vat_mv_line, 'amount': vat_amount,
                                                   'account_id': vat_account_id, 'tax_tag_ids': vt_line_tag_ids}})
        for line_dict in mv_line_data_list:
            for key, data_dict in line_dict.items():
                if data_dict.get('amount') == 0.0:
                    continue
                move_line_list.append({'name': data_dict.get('name'),
                                       'account_id': data_dict.get('account_id'),
                                       'analytic_account_id': data_dict.get('analytic_account_id', False),
                                       'balance': -data_dict.get('amount'),
                                       'tax_ids': [[6, None, [] if key == 'vat_line' else tax_id.ids]],
                                       'tax_tag_ids': [[6, None, data_dict.get('tax_tag_ids')]]})
        return move_line_list

    def reconcile_remaining_transactions(self):
        """This function is used to reconcile remaining transaction of settlement report
            @author: Dimpal added on 15/oct/2019

        Migration done by twinkalc on 28 sep, 2020,
        Changes related to process for reconcile the ending balance lines,
        post the bank statement and filter the statement line which needs to
        reconcile.
        """
        statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        transaction_obj = self.env[ACCOUNT_TRANSACTION_LINE_EPT]
        tax_obj = self.env['account.tax']
        self.search_and_reconile_ending_balance_line()
        self.remaining_order_refund_lines()
        self._cr.commit()
        fees_transaction_dict = {}
        trans_line_ids = transaction_obj.search([('seller_id', '=', self.seller_id.id)])
        for trans_line_id in trans_line_ids:
            transaction_type_id = trans_line_id.transaction_type_id
            if trans_line_id.id in fees_transaction_dict:
                fees_transaction_dict[transaction_type_id.amazon_code].append(
                    trans_line_id.id)
            else:
                fees_transaction_dict.update({transaction_type_id.amazon_code: [trans_line_id.id]})
        statement_lines = self.statement_id.line_ids.filtered(lambda x: not x.is_reconciled and x.amazon_code).ids
        rei_lines = []
        for x in range(0, len(statement_lines), 10):
            lines = statement_lines[x:x + 10]
            for line_id in lines:
                move_line_list = []
                line = statement_line_obj.browse(line_id)
                trans_line_id = fees_transaction_dict.get(line.amazon_code)
                if not trans_line_id:
                    continue
                trans_line = transaction_obj.browse(trans_line_id)
                if trans_line[0].transaction_type_id.is_reimbursement:
                    rei_lines.append(line.id)
                    continue
                account_id = trans_line[
                    0].account_id.id if trans_line else self.statement_id.company_id.account_journal_payment_credit_account_id.id
                analytic_account_id = trans_line.analytic_account_id.id if trans_line.analytic_account_id else False
                if not account_id:
                    continue
                tax_id = trans_line[0].tax_id if trans_line else tax_obj
                if tax_id:
                    move_line_list = self.reconcile_other_transaction_line_with_taxes(
                        tax_id, line_id, move_line_list, account_id, analytic_account_id)
                else:
                    move_line_list.append({
                        'name': line.name,
                        'account_id': account_id,
                        'analytic_account_id': analytic_account_id,
                        'balance': -line.amount,
                        'tax_ids': []
                    })
                line.reconcile(lines_vals_list=move_line_list)
            self._cr.commit()
        self.reconcile_amazon_statement_lines(rei_lines, fees_transaction_dict)
        return True

    def get_settlement_start_and_end_date(self, row, start_date, end_date):
        """
        This method will return report start and end date
        """
        if not start_date:
            start_date = self.format_amz_settlement_report_date(row.get('settlement-start-date'))
        if not end_date:
            end_date = self.format_amz_settlement_report_date(row.get('settlement-end-date'))
        return start_date, end_date

    def create_amazon_report_attachment(self, response):
        """
        Migration done by twinkalc on 28 sep, 2020,
        :param response : Response of settlement report data.
        This Method will process the response to prepare attachment.
        """
        if response:
            response = response.get('document', '')
            reader = csv.DictReader(response.split('\n'), delimiter='\t')
            start_date = ''
            end_date = ''
            currency_id = False
            marketplace = ''
            for row in reader:
                if marketplace:
                    break
                start_date, end_date = self.get_settlement_start_and_end_date(row, start_date, end_date)
                if not currency_id:
                    currency_id = self.env['res.currency'].search([('name', '=', row.get('currency', ''))])
                if not marketplace:
                    marketplace = row.get('marketplace-name', '')
            self.prepare_attachments(response, marketplace, start_date, end_date, currency_id)
        return True

    @staticmethod
    def format_amz_settlement_report_date(date):
        """
        Usage of this method is to properly format settlement report dates.
        date will be in this format 21.01.2021 03:09:54 UTC OR 2021-01-21 03:09:53 UTC
        :param date: DateTime in String UTC format
        :return: datetime()
        """
        try:
            formatted_date = datetime.strptime(date, '%d.%m.%Y %H:%M:%S UTC')
        except Exception as ex:
            _logger.error(ex)
            formatted_date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S UTC')
        return formatted_date

    def prepare_attachments(self, data, marketplace, start_date, end_date, currency_rec):
        """
        Added by Udit
        :param data: Attachment data.
        :param marketplace: Market place.
        :param start_date: Selected start date in specific format.
        :param end_date: Selected end date in specific format.
        :param currency_rec: Currency from amazon.
        :return: This method will create attachments, attach it to settlement
        report's record and create a log note.
        Migration done by twinkalc on 28 sep, 2020,
        """
        instance = self.env['amazon.marketplace.ept'].find_instance(self.seller_id, marketplace)
        data = data.encode('utf-8')
        result = base64.b64encode(data)
        file_name = "Settlement_report_" + time.strftime("%Y_%m_%d_%H%M%S") + '.csv'
        attachment = self.env['ir.attachment'].create({
            'name': file_name,
            'datas': result,
            'res_model': 'mail.compose.message',
        })
        self.message_post(body=_("<b>Settlement Report Downloaded</b>"),
                          attachment_ids=attachment.ids)
        self.write({'attachment_id': attachment.id,
                    'start_date': start_date and start_date.strftime(DATE_YMD),
                    'end_date': end_date and end_date.strftime(DATE_YMD),
                    'currency_id': currency_rec and currency_rec[0].id or False,
                    'instance_id': instance and instance[0].id or False
                    })

    @staticmethod
    def get_amz_settlement_posted_date(posted_date):
        """
        Added method to get posted date
        """
        try:
            posted_date = datetime.strptime(posted_date, '%d.%m.%Y')
        except Exception as ex:
            _logger.error(ex)
            posted_date = datetime.strptime(posted_date, DATE_YMD)
        return posted_date

    @staticmethod
    def get_amazon_order_list_item_price(key, amount, order_list_item_price):
        """
        This method will used to prepare an list of order item price
        """
        if not order_list_item_price.get(key, 0.0):
            order_list_item_price.update({key: amount})
        else:
            existing_amount = order_list_item_price.get(key, 0.0)
            order_list_item_price.update({key: existing_amount + amount})
        return order_list_item_price

    def process_settlement_report_file(self):
        """
        Process work for fetch data from settlement report,create bank statement
        and statement line
        @author: Deval Jagad (15/11/2019)
        Migration done by twinkalc on 28 sep, 2020,
        """
        self.ensure_one()
        ir_cron_obj = self.env['ir.cron']
        if not self._context.get('is_auto_process', False):
            ir_cron_obj.with_context(**{'raise_warning': True}).find_running_schedulers( \
                'ir_cron_auto_process_settlement_report_seller_', self.seller_id.id)
        self.check_instance_configuration_and_attachment_file()
        imp_file = StringIO(base64.b64decode(self.attachment_id.datas).decode())
        content = imp_file.read()
        delimiter = ('\t', csv.Sniffer().sniff(content.splitlines()[0]).delimiter)[bool(content)]
        settlement_reader = csv.DictReader(content.splitlines(), delimiter=delimiter)
        journal = self.instance_id.settlement_report_journal_id
        seller = self.seller_id
        bank_statement = False
        settlement_id = ''
        order_list_item_price = {}
        order_list_item_fees = {}
        refund_list_item_price = {}
        create_or_update_refund_dict = {}
        amazon_product_obj = self.env['amazon.product.ept']
        partner_obj = self.env['res.partner']
        amazon_other_transaction_list = {}
        product_dict = {}
        order_dict = {}

        for row in settlement_reader:
            settlement_id = row.get('settlement-id', False)
            if not bank_statement:
                bank_statement = self.create_settlement_report_bank_statement(row, journal, settlement_id)
                if not bank_statement:
                    break
            if not row.get('transaction-type', ''):
                continue
            order_ref = row.get('order-id', '')
            shipment_id = row.get('shipment-id', '')
            order_item_code = row.get('order-item-code', '').lstrip('0')
            posted_date = row.get('posted-date', '')
            fulfillment_by = row.get('fulfillment-id', '')
            adjustment_id = row.get('adjustment-id', '')
            posted_date = self.get_amz_settlement_posted_date(posted_date)
            amount = float(row.get('amount', 0.0).replace(',', '.'))
            if row.get('transaction-type', '') in ['Order', 'Refund']:
                if row.get('amount-description', '').__contains__('MarketplaceFacilitator') or \
                        row.get('amount-description', '').__contains__('LowValueGoods') or \
                        row.get('amount-type', '') == 'ItemFees':
                    order_list_item_fees = self.prepare_order_list_item_fees_ept( \
                        row, settlement_id, amount, posted_date, order_list_item_fees)
                    continue
                amz_order = self.get_settlement_report_amazon_order_ept(row)
                order_ids = order_dict.get((order_ref, shipment_id, order_item_code))
                if not order_ids:
                    order_ids = tuple(amz_order.ids)
                    order_dict.update({(order_ref, shipment_id, order_item_code): order_ids})

                partner = partner_obj.with_context(is_amazon_partner=True)._find_accounting_partner(
                    amz_order.mapped('partner_id'))

                if row.get('transaction-type', '') == 'Order':
                    key = (order_ref, order_ids, posted_date, fulfillment_by, partner.id, shipment_id)
                    order_list_item_price = self.get_amazon_order_list_item_price(key, amount, order_list_item_price)

                elif row.get('transaction-type', '') == 'Refund':
                    product_id = product_dict.get(row.get('sku', ''))
                    if not product_id:
                        amazon_product = amazon_product_obj.search([('seller_sku', '=', row.get('sku', '')),
                                                                    ('instance_id', '=', self.instance_id.id)], limit=1)
                        product_id = amazon_product.product_id.id
                        product_dict.update({row.get('sku', ''): amazon_product.product_id.id})
                    key = (order_ref, order_ids, posted_date, fulfillment_by, partner.id, adjustment_id)
                    if not refund_list_item_price.get(key, 0.0):
                        refund_list_item_price.update({key: amount})
                    else:
                        existing_amount = refund_list_item_price.get(key, 0.0)
                        refund_list_item_price.update({key: existing_amount + amount})

                    create_or_update_refund_dict = self.get_settlement_refund_dict_ept(row, key,
                                                                                       product_id,
                                                                                       create_or_update_refund_dict)
            else:
                if row.get('amount-type') in ['other-transaction', 'FBA Inventory Reimbursement']:
                    key = (row.get('amount-type', ''), posted_date, row.get('amount-description', ''), settlement_id)
                elif row.get('transaction-type') in ['Order_Retrocharge']:
                    key = (row.get('transaction-type'), posted_date, order_ref, settlement_id)
                else:
                    key = (row.get('amount-type', ''), posted_date, '', settlement_id)
                existing_amount = amazon_other_transaction_list.get(key, 0.0)
                amazon_other_transaction_list.update({key: existing_amount + amount})

        if bank_statement:
            self.make_amazon_fee_entry(bank_statement, order_list_item_fees)
            if amazon_other_transaction_list:
                self.make_amazon_other_transactions(seller, bank_statement, amazon_other_transaction_list)

            if order_list_item_price:
                self.process_settlement_orders(bank_statement, settlement_id, order_list_item_price)

            if refund_list_item_price:
                self.process_settlement_refunds(bank_statement.id, refund_list_item_price)

            # Create manually refund in ERP whose returned not found in the system
            if create_or_update_refund_dict:
                self.create_refund_invoices(create_or_update_refund_dict, bank_statement)

            self.write({'statement_id': bank_statement.id, 'state': 'imported'})
        return True

    def get_settlement_report_amazon_order_ept(self, row):
        """
        Added by twinkalc on 28 sep, 2020,
        @param : row - contain the settlement report vals
        This method will get the amazon order.
        """

        sale_order_obj = self.env[SALE_ORDER]
        stock_move_obj = self.env['stock.move']

        order_ref = row.get('order-id', '')
        shipment_id = row.get('shipment-id', '')
        order_item_code = row.get('order-item-code', '').lstrip('0')
        fulfillment_by = row.get('fulfillment-id', '')

        if fulfillment_by == 'MFN':
            amz_order = sale_order_obj.search(
                [('amz_order_reference', '=', order_ref),
                 ('amz_instance_id', '=', self.instance_id.id),
                 ('amz_fulfillment_by', '=', 'FBM'),
                 ('state', '!=', 'cancel')])
        else:
            domain = [
                ('amazon_instance_id', '=', self.instance_id.id),
                ('amazon_order_reference', '=', order_ref),
                ('amazon_order_item_id', '=', order_item_code),
                ('state', '=', 'done')]
            if shipment_id:
                domain.append(('amazon_shipment_id', '=', shipment_id))
            stock_move = stock_move_obj.search(domain)
            if not stock_move:
                stock_move = self.search_amazon_order_stock_move_ept(row, domain)
            amz_order = stock_move.mapped('sale_line_id').mapped('order_id')
        return amz_order

    def search_amazon_order_stock_move_ept(self, row, domain):
        """
        Define method for get amazon order stock move.
        :param: row: contain the settlement report vals
        :param: domain: domain use for find amazon order stock move
        :return: stock.move() object
        @author: Kishan Sorani on date 08-Nov-2021
        """
        stock_move_obj = self.env['stock.move']
        stock_move = stock_move_obj
        amazon_product_obj = self.env['amazon.product.ept']
        amazon_product = amazon_product_obj.search([('seller_sku', '=', row.get('sku', '')),
                                                    ('instance_id', '=', self.instance_id.id)], limit=1)
        if amazon_product:
            product_id = amazon_product.product_id.id
            domain.pop(2)
            domain.append(('product_id', '=', product_id))
            stock_move = stock_move_obj.search(domain, limit=1)
        return stock_move

    @staticmethod
    def prepare_order_list_item_fees_ept(row, settlement_id, amount, posted_date, order_list_item_fees):
        """
        Added by twinkalc on 28 sep, 2020,
        @param : row - contain the settlement report vals
        @param : amount - contain the amount of settlement report.
        @param : order_list_item_fees - list of item fees
        This method will prepare the order item list
        """
        key = (settlement_id, posted_date, row.get('amount-description', ''))
        if not order_list_item_fees.get(key, 0.0):
            order_list_item_fees.update({key: amount})
        else:
            existing_amount = order_list_item_fees.get(key, 0.0)
            order_list_item_fees.update({key: existing_amount + amount})
        return order_list_item_fees

    @staticmethod
    def get_settlement_refund_dict_ept(row, key, product_id, create_or_update_refund_dict):
        """
        Added by twinkalc on 28 sep, 2020,
        @param : row - contain the settlement report vals
        @param : key - key if refund dict.
        @param : product_id - refund dict product
        This method will prepare the refund dict based on that create refund invoice in ERP.
        """
        principal = 0
        tax = 0
        if str(row.get('amount-description', '').lower().find('tax')) != '-1':
            tax = float(row.get('amount', 0.0).replace(',', '.')) or 0
        else:
            principal = float(row.get('amount', 0.0).replace(',', '.')) or 0
        amount = [principal, tax]

        if not create_or_update_refund_dict.get(key, False):
            create_or_update_refund_dict.update({key: {product_id: amount}})
        else:
            existing_amount = create_or_update_refund_dict.get(key, {}).get(
                product_id, [0.0, 0.0])
            principle = existing_amount[0] + amount[0]
            tax = existing_amount[1] + amount[1]
            amount = [principle, tax]
            create_or_update_refund_dict.get(key, {}).update({product_id: amount})
        return create_or_update_refund_dict

    def check_instance_configuration_and_attachment_file(self):
        """
        This method check in settlement report attachment exist or not
        Also check configuration of instance, Settlement Report Journal and Currency
        @author: Deval Jagad (18/11/2019)
        """
        if not self.attachment_id:
            raise UserError(_("There is no any report are attached with this record."))
        if not self.instance_id:
            raise UserError(_("Please select the Instance in report."))
        if not self.instance_id.settlement_report_journal_id:
            raise UserError(_("You have not configured Settlement report Journal in Instance. "
                              "\nPlease configured it first."))
        currency_id = self.instance_id.settlement_report_journal_id.currency_id.id or \
                      self.seller_id.company_id.currency_id.id or False
        if currency_id != self.currency_id.id:
            raise UserError(_("Report currency and Currency in Instance Journal are different. "
                              "\nMake sure Report currency and Instance Journal currency must be same."))

    def check_settlement_report_exist(self, settlement_id):
        """
        Process check bank statement record and settlement record exist or not
        @:param - settlement_id - unique id from csv file
        @author: Deval Jagad (20/11/2019)
        Migration done by twinkalc on 28 sep, 2020,
        """
        bank_statement_obj = self.env[ACCOUNT_BANK_STATEMENT]
        bank_statement_exist = bank_statement_obj.search( \
            [('settlement_ref', '=', settlement_id)])
        if bank_statement_exist:
            settlement_exist = self.search([('statement_id', '=', bank_statement_exist.id)])
            if settlement_exist and settlement_exist.id == self.id:
                return bank_statement_exist
            if settlement_exist:
                self.write({'already_processed_report_id': settlement_exist.id,
                            'state': 'duplicate'})
            else:
                self.write({'statement_id': bank_statement_exist.id, 'state': 'processed'})
            return bank_statement_exist
        return False

    def create_settlement_report_bank_statement_line(self, total_amount, settlement_id,
                                                     bank_statement, deposit_date):
        """
        Process create "Total amount" bank statement line
        @:param - total amount - total amount of settlement report
        @:param - settlement_id - unique id from csv file
        @:param - bank_statement - account.bank.statement record create for settlement report
        @:param - deposite_date - deposite date of settlement report
        @author: Deval Jagad (16/11/2019)
        Migration done by twinkalc on 28 sep, 2020,
        """
        if self.instance_id.ending_balance_account_id and float(total_amount) != 0.0:
            ending_payment_ref = '%s/%s/%s' % (settlement_id,
                                               self.instance_id.ending_balance_description or ENDING_BALANCE_DESC,
                                               bank_statement.name)
            bank_statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
            bank_line_vals = {
                "is_ending_balance_entry": True,
                'payment_ref': ending_payment_ref,
                'partner_id': False,
                'amount': -float(total_amount),
                'statement_id': bank_statement.id,
                'date': deposit_date,
                'sequence': 1000
            }
            bank_statement_line_obj.create(bank_line_vals)

    def create_settlement_report_bank_statement(self, row, journal, settlement_id):

        """
        Process first check bank statement exist or not
        If not exist then create bank statement and
        create 'Total amount' bank statement line
        @:param - row - whole row of csv file
        @:param - journal - configure in Amazon Marketplace
        @:param - settlement_id - unique id from csv file
        @author: Deval Jagad (16/11/2019)
        Migration done by twinkalc on 28 sep, 2020,
        """
        bank_statement_obj = self.env[ACCOUNT_BANK_STATEMENT]
        deposit_date = self.format_amz_settlement_report_date(row.get('deposit-date', ''))
        total_amount = float(row.get('total-amount', 0.0).replace(',', '.'))
        start_date = self.start_date
        end_date = self.end_date

        bank_statement_exist = self.check_settlement_report_exist(settlement_id)
        if bank_statement_exist:
            return False
        name = '%s %s to %s ' % (self.instance_id.marketplace_id.name, start_date, end_date)
        vals = {
            'settlement_ref': settlement_id,
            'journal_id': journal.id,
            'date': self.end_date,
            'name': name,
            'balance_end_real': total_amount,
        }
        if self.instance_id.ending_balance_account_id:
            vals.update({'balance_end_real': 0.0})
        bank_statement = bank_statement_obj.create(vals)
        self.create_settlement_report_bank_statement_line(total_amount, settlement_id,
                                                          bank_statement, deposit_date)
        return bank_statement

    @staticmethod
    def convert_move_amount_currency(bank_statement, moveline, amount, date):
        """This function is used to convert currency
            @author: Dimpal added on 14/oct/2019
            Migration done by twinkalc on 28 sep, 2020,
            @:param bank_statement : bank statement
            @:param moveline : account.move.line record passed during
            reconciliation
            @:param amount : amount needs to convert in currency
            @date : date of statement line
        """
        amount_currency = 0.0
        if moveline.company_id.currency_id.id != bank_statement.currency_id.id:
            amount_currency = moveline.currency_id._convert(moveline.amount_currency,
                                                            bank_statement.currency_id,
                                                            bank_statement.company_id,
                                                            date)
        elif (moveline.move_id and moveline.move_id.currency_id.id != bank_statement.currency_id.id):
            amount_currency = moveline.move_id.currency_id._convert(amount,
                                                                    bank_statement.currency_id,
                                                                    bank_statement.company_id,
                                                                    date)
        currency = moveline.currency_id.id
        return currency, amount_currency

    @api.model
    def process_settlement_refunds(self, bank_statement_id, refunds):
        """
        Migration done by twinkalc on 28 sep, 2020,
        @:param bank_statement_id : bank statement
        @:param settlement_id : settlement record id
        @:param refunds : refund item list to create refund bank statement lines.
        @:return : refund invoice dict
        """
        bank_statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        refund_invoice_dict = defaultdict(dict)
        for order_key, refund_amount in refunds.items():
            orders = order_key[1]
            amz_order = orders[0] if len(orders) > 1 else orders
            partner_id = order_key[4]
            date_posted = order_key[2]
            if not refund_amount:
                continue
            adjustment_id = (order_key[5] + '/') if order_key[5] else ''
            refund_name = 'Refund_' + adjustment_id + order_key[0]
            bank_line_vals = {
                'payment_ref': refund_name,
                'partner_id': partner_id,
                'amount': refund_amount,
                'statement_id': bank_statement_id,
                'date': date_posted,
                'is_refund_line': True,
                'sale_order_id': amz_order
            }
            bank_statement_line_obj.create(bank_line_vals)
        return refund_invoice_dict

    def make_amazon_fee_entry(self, bank_statement, fees_type_dict):
        """
        Migration done by twinkalc on 28 sep, 2020,
        @:param bank_statement : bank statement
        @:fees_type_dict : to create amazon fees bank statement lines
        records.
        """
        bank_statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        for key, value in fees_type_dict.items():
            if value != 0:
                name = "%s/%s/%s" % (key[0], key[1], key[2])
                bank_line_vals = {
                    'payment_ref': name,
                    'amount': value,
                    'statement_id': bank_statement.id,
                    'date': key[1],
                    'amazon_code': key[2]
                }
                bank_statement_line_obj.create(bank_line_vals)
        return True

    @staticmethod
    def get_amz_other_transaction_name(transaction):
        """
        This method will prepare name of the other transaction statement line
        """
        trans_type = transaction[0]
        trans_id = transaction[2]
        date_posted = transaction[1]
        settlement_ref = transaction[3]
        name = ''
        if trans_type:
            name = "%s/%s/%s/%s" % (settlement_ref, trans_type, trans_id, date_posted)
            if not trans_id:
                name = "%s/%s/%s" % (settlement_ref, trans_type, date_posted)
        return name

    def make_amazon_other_transactions(self, seller, bank_statement, other_transactions):
        """
        Migration done by twinkalc on 28 sep, 2020,
        @:param bank_statement : bank statement.
        @:param other_transactions : amazon other transactions list.
        This method is used to create bank statement lines of other
        transactions.
        """
        transaction_obj = self.env[ACCOUNT_TRANSACTION_LINE_EPT]
        bank_statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        trans_line_ids = transaction_obj.search([('seller_id', '=', seller.id)])
        fees_transaction_list = {trans_line_id.transaction_type_id.amazon_code: trans_line_id.id for trans_line_id in
                                 trans_line_ids}
        bank_line_values = []
        for transaction, amount in other_transactions.items():
            if amount == 0.00:
                continue
            trans_type = transaction[0]
            trans_id = transaction[2]
            date_posted = transaction[1]
            settlement_ref = transaction[3]
            trans_type = trans_id if trans_type in ['other-transaction',
                                                    'FBA Inventory Reimbursement'] else trans_type
            trans_type_line_id = fees_transaction_list.get(trans_type) or fees_transaction_list.get(
                trans_id)
            trans_line = trans_type_line_id and transaction_obj.browse(trans_type_line_id)
            name = self.get_amz_other_transaction_name(transaction)
            if (not trans_line) or (
                    trans_line and not trans_line.transaction_type_id.is_reimbursement):
                bank_line_values.append({
                    'payment_ref': name,
                    'amount': amount,
                    'statement_id': bank_statement.id,
                    'date': date_posted,
                    'amazon_code': trans_type
                })
            elif trans_line.transaction_type_id.is_reimbursement:
                name = "%s/%s/%s/%s" % (settlement_ref, trans_type, date_posted, 'Reimbursement')
                self.make_amazon_reimbursement_line_entry(bank_statement, date_posted, trans_type,
                                                          {name: amount})
        bank_statement_line_obj.create(bank_line_values)
        return True

    def create_amazon_reimbursement_invoice_line(self, seller, reimbursement_invoice,
                                                 name='REVERSAL_REIMBURSEMENT', amount=0.0,
                                                 trans_line=False):
        """
        @author:  Added by Dimpal on 12/oct/2019
        Migration done by twinkalc on 28 sep, 2020,
        @:param : seller - seller selected in settlement record.
        @param : reimbursement_invoice - reimbursement invoice record.
        @param : name - displayed into the invoice line
        @param : amount - reimbursement invoice line unit price
        @param : trans_line - amazon transaction line
        This function is used to create reimbursement invoice lines
        """
        invoice_line_obj = self.env[ACCOUNT_MOVE_LINE]
        reimbursement_product = seller.reimbursement_product_id
        tax_id = False
        vals = {'product_id': reimbursement_product.id,
                'name': name,
                'move_id': reimbursement_invoice.id,
                'price_unit': amount,
                'quantity': 1,
                'product_uom_id': reimbursement_product.uom_id.id, }
        if self.currency_id.id != self.company_id.currency_id.id:
            vals.update({'currency_id': self.currency_id.id})
        new_record = invoice_line_obj.new(vals)
        new_record._onchange_product_id()
        retval = invoice_line_obj._convert_to_write(
            {name: new_record[name] for name in new_record._cache})
        retval.update({'price_unit': amount})
        account_id = trans_line and trans_line.account_id.id or \
                     self.instance_id.company_id.account_journal_payment_credit_account_id.id or False
        if account_id:
            retval.update({'account_id': account_id})
        if trans_line and trans_line.tax_id:
            tax_id = trans_line.tax_id.id
        if tax_id:
            retval.update({'tax_ids': [(6, 0, [tax_id])]})
        invoice_line_obj.with_context(**{'check_move_validity': False}).create(retval)
        return True

    def create_amazon_reimbursement_invoice(self, bank_statement, seller, date_posted,
                                            invoice_type):
        """
        @author:  Added by Dimpal on 12/oct/2019
        Migration done by twinkalc on 28 sep, 2020,
        @:param : bank_statement - bank statement record.
        @:param : seller - seller selected in settlement record.
        @:param : date_posted - invoice date
        @:param : invoice_type - invoice type
        return : record of created reimbursement invoice.
        This function is used to create reimbursement invoice
        """
        invoice_obj = self.env[ACCOUNT_MOVE]
        partner_id = seller.reimbursement_customer_id.id
        fiscal_position_id = seller.reimbursement_customer_id.property_account_position_id.id
        journal_id = seller.sale_journal_id.id
        invoice_vals = {
            'move_type': invoice_type,
            'ref': bank_statement.name,
            'partner_id': partner_id,
            'journal_id': journal_id,
            'currency_id': self.currency_id.id,
            'amazon_instance_id': self.instance_id.id,
            'fiscal_position_id': fiscal_position_id,
            'company_id': self.company_id.id,
            'user_id': self._uid or False,
            'date': date_posted,
            'seller_id': seller.id,
        }
        reimbursement_invoice = invoice_obj.create(invoice_vals)
        return reimbursement_invoice

    def make_amazon_reimbursement_line_entry(self, bank_statement, date_posted, trans_type, fees_type_dict):
        """
        Migration done by twinkalc on 28 sep, 2020,
        @:param bank_statement : bank statement
        @:param date_posted : statement line date
        @:fees_type_dict : reimbursement line dict
        records.
        """
        bank_statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        bank_line_vals = [{'payment_ref': fee_type,
                           'amount': amount,
                           'statement_id': bank_statement.id,
                           'date': date_posted,
                           'amazon_code': trans_type} for fee_type, amount in fees_type_dict.items() if amount != 0.00]
        statement_line = bank_statement_line_obj.create(bank_line_vals)
        return statement_line

    def process_settlement_orders(self, bank_statement, settlement_id, orders_list):
        """
        Migration done by twinkalc on 28 sep, 2020,
        @:param bank_statement - bank statement record
        @:param settlement_id - settlement record id
        @:param orders - order list
        This method will process to create order invoices
        and bank statement lines
        """
        sale_order_obj = self.env[SALE_ORDER]
        bank_statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]

        bank_line_vals = [{
                           'payment_ref': "%s/%s/%s" % (settlement_id, order_key[5], order_key[0]), 'partner_id':
                               order_key[4], 'amount': invoice_total,
                           'statement_id': bank_statement.id, 'date': order_key[2],
                           'sale_order_id': sale_order_obj.browse(order_key[1]).filtered(
                               lambda l, invoice_total=invoice_total: round(l.amount_total, 10) == round(invoice_total, 10)) and
                                            sale_order_obj.browse(order_key[1])[0].id or sale_order_obj.browse(\
                               order_key[1]).id if order_key[4] != 'MFN' else sale_order_obj.browse(order_key[1]).id}
                          for order_key, invoice_total in orders_list.items() if invoice_total != 0.0]
        splited_vals = []
        vals = []
        for line in bank_line_vals:
            if len(vals) <= 500:
                vals.append(line)
            else:
                splited_vals.append(vals)
                vals = [line]
        splited_vals.append(vals)

        for line in splited_vals:
            bank_statement_line_obj.create(line)

        return True

    def check_or_create_invoice_if_not_exist(self, amz_order):
        """
        Migration done by twinkalc on 28 sep, 2020,
        @:param amz_order - amazon order.
        This method will check or create invoice of amazon order.
        @Note : default_fba_partner_id is fetched according to seller wise.
        """
        for order in amz_order:
            if order.amz_instance_id.seller_id.def_fba_partner_id.id == order.partner_id.id or order.state == 'cancel':
                continue
            order_invoices = order.invoice_ids.filtered(lambda l: l.move_type == 'out_invoice' and l.state not in ('cancel'))
            if not order_invoices and order.state != 'cancel':
                self.create_invoice_if_not_exist(order)
            # Confirm All Draft Invoices in Sale Order.
            for invoice in order.invoice_ids:
                if invoice.state == 'draft' and invoice.move_type == 'out_invoice':
                    invoice.action_post()
        return True

    @staticmethod
    def create_invoice_if_not_exist(order):
        """
        For FBA Orders Invoices must be as per Shipment Id's, Invoice amount must be same as Shipment on sale order.
        So For FBA Orders, Different Invoices are required and For FBM Orders Normal Odoo workflow will work.
        :param order: sale.order()
        """
        try:
            shipment_ids = {}
            # Order is process from Shipping report then create invoices as per shipment id
            if order.amz_fulfillment_by == 'FBA' and order.amz_shipment_report_id:
                for move in order.order_line.move_ids:
                    if move.amazon_shipment_id in shipment_ids:
                        shipment_ids.get(move.amazon_shipment_id).append(move.amazon_shipment_item_id)
                    else:
                        shipment_ids.update({move.amazon_shipment_id: [move.amazon_shipment_item_id]})
                for shipment, shipment_item in list(shipment_ids.items()):
                    to_invoice = order.order_line.filtered(lambda l: l.qty_to_invoice != 0.0)
                    if to_invoice:
                        # The context will used in prepare invoice vals and create invoice as per shipment id.
                        order.with_context({'shipment_item_ids': shipment_item})._create_invoices()
            else:
                # Used for FBM Orders
                order._create_invoices()
        except Exception as ex:
            _logger.error(ex)

    @staticmethod
    def reconcile_amazon_reimbursement_line(reimbursement_line, mv_line_dicts, currency_ids):
        """
        This method used to reconcile the reimbursement line
        """
        if currency_ids:
            currency_ids = list(set(currency_ids))
            if len(currency_ids) == 1:
                statement_currency = reimbursement_line.journal_id.currency_id and \
                                     reimbursement_line.journal_id.currency_id.id or \
                                     reimbursement_line.company_id.currency_id and \
                                     reimbursement_line.company_id.currency_id.id
                if not currency_ids[0] == statement_currency:
                    reimbursement_line.write({'currency_id': currency_ids[0]})
        if mv_line_dicts:
            reimbursement_line.reconcile(lines_vals_list=mv_line_dicts)
        return True

    def reconcile_reimbursement_invoice(self, reimbursement_invoices, reimbursement_line, bank_statement):
        """
        @author: Dimpal added on 14/oct/2019
        Migration done by twinkalc on 28 sep, 2020,
        @:param reimbursement_invoices - amazon order.
        @:param reimbursement_line - reimbursement line
        @:param bank_statement - bank statement record.
        This function is used to reconcile reimbursement invoice
        """
        ctx = {'check_move_validity': False}
        move_line_obj = self.env[ACCOUNT_MOVE_LINE]
        for reimbursement_invoice in reimbursement_invoices:
            if reimbursement_invoice.state == 'draft':
                reimbursement_invoice.with_context(**ctx)._onchange_invoice_line_ids()
                reimbursement_invoice.with_context(**ctx)._recompute_dynamic_lines(recompute_all_taxes=True)
                reimbursement_invoice.action_post()
        account_move_ids = list(map(lambda x: x.id, reimbursement_invoices))
        move_lines = move_line_obj.search([('move_id', 'in', account_move_ids),
                                           ('account_id.user_type_id.type', '=', 'receivable'),
                                           ('reconciled', '=', False)])
        mv_line_dicts = []
        move_line_total_amount = 0.0
        currency_ids = []
        for moveline in move_lines:
            amount = moveline.debit - moveline.credit
            amount_currency = 0.0
            if moveline.amount_currency:
                currency, amount_currency = self.convert_move_amount_currency(bank_statement,
                                                                              moveline, amount,
                                                                              reimbursement_line.date)
                if currency:
                    currency_ids.append(currency)

            if amount_currency:
                amount = amount_currency
            mv_line_dicts.append({
                'name': moveline.move_id.name,
                'id': moveline.id,
                'balance': -amount,
                'currency_id': moveline.currency_id.id
            })
            move_line_total_amount += amount

        if round(reimbursement_line.amount, 10) == round(move_line_total_amount, 10) and (
                not reimbursement_line.currency_id or reimbursement_line.currency_id.id == bank_statement.currency_id.id):
            self.reconcile_amazon_reimbursement_line(reimbursement_line, mv_line_dicts, currency_ids)
        return True

    def search_settlement_refund_invoices(self, order):
        """
        This method will find the refund invoices
        """
        statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        refund_exist = order.invoice_ids.filtered(
            lambda l: l.move_type == 'out_refund' and l.state not in ('cancel'))
        if refund_exist:
            is_refund_line_ids = statement_line_obj.search([('sale_order_id', '=', order.id),
                                                            ('is_refund_line', '=', True)])
            reserved_refund_invoice_ids = is_refund_line_ids.mapped('refund_invoice_id').ids
            refund_exist = refund_exist.filtered(lambda l: l.id not in reserved_refund_invoice_ids)
            refund_exist = refund_exist[0] if refund_exist else False
        return refund_exist

    @staticmethod
    def process_settlement_draft_invoices(invoice):
        """
        This method will re-compute the lines and posted the draft invoices
        """
        invoice.with_context(**{'check_move_validity': False})._onchange_invoice_line_ids()
        invoice.with_context(**{'check_move_validity': False})._recompute_dynamic_lines(recompute_all_taxes=True)
        invoice.action_post()
        return True

    @api.model
    def create_refund_invoices(self, refund_list_item_price, bank_statement):
        """
        Migration done by twinkalc on 28 sep, 2020,
        @:param refund_list_item_price - refund list.
        @:param bank_statement : bank statement record.
        This method is used to create refund invoices
        """

        obj_invoice_line = self.env[ACCOUNT_MOVE_LINE]
        sale_order_obj = self.env[SALE_ORDER]
        obj_invoice = self.env[ACCOUNT_MOVE]
        statement_line_obj = self.env[ACCOUNT_BANK_STATEMENT_LINE]
        refund_obj = self.env['account.move.reversal']

        ctx = {'check_move_validity': False}
        for order_key, product_amount in refund_list_item_price.items():
            if not order_key[1]:
                continue
            order = sale_order_obj.browse(order_key[1])
            if len(order.ids) > 1:
                order = order[0]
            if order.state == 'cancel':
                continue
            date_posted = order_key[2]
            product_ids = list(product_amount.keys())
            refund_exist = self.search_settlement_refund_invoices(order)
            if not refund_exist:
                invoices = order.invoice_ids.filtered(
                    lambda l: l.move_type == 'out_invoice' and l.state == 'posted').invoice_line_ids.filtered(\
                    lambda l, product_ids=product_ids: l.product_id.id in product_ids).mapped('move_id')
                if not invoices:
                    try:
                        self.check_or_create_invoice_if_not_exist(order)
                    except Exception as ex:
                        _logger.error(ex)
                        continue
                    invoices = order.invoice_ids.filtered(
                        lambda l: l.move_type == 'out_invoice' and l.state != 'cancel').invoice_line_ids.filtered(\
                        lambda l, product_ids=product_ids: l.product_id.id in product_ids).mapped('move_id')
                if not invoices:
                    continue
                credit_ctx = {'active_ids': invoices[0].ids, 'active_id': invoices[0].id, 'active_model': ACCOUNT_MOVE}
                credit_note_wizard = refund_obj.with_context(credit_ctx).create({
                    'refund_method': 'refund',
                    'date': date_posted,
                    'reason': 'Refund Process Amazon Settlement Report',
                    'journal_id': invoices[0].journal_id.id,
                })
                refund_invoices = obj_invoice.browse(credit_note_wizard.reverse_moves()['res_id'])
                for refund_invoice in refund_invoices:
                    extra_invoice_lines = obj_invoice_line.search(
                        [('move_id', '=', refund_invoice.id), ('product_id', 'not in', product_ids)])
                    if extra_invoice_lines:
                        extra_invoice_lines.with_context(**ctx).unlink()
                    for product_id, amount in product_amount.items():
                        unit_price = abs(amount[0] + amount[1])
                        taxargs = {}
                        invoice_lines = refund_invoice.invoice_line_ids.filtered(
                            lambda x, product_id=product_id: x.product_id.id == product_id)
                        exact_line = False
                        if len(invoice_lines.ids) > 1:
                            exact_line = invoice_lines[0]
                            if order.amz_instance_id.is_use_percent_tax:
                                taxargs = self.get_amz_refund_unit_price_ept(exact_line, amount)
                                unit_price = taxargs.get('price_unit', 0.0)
                            if exact_line:
                                other_lines = invoice_lines.filtered(
                                    lambda invoice_line, exact_line=exact_line: invoice_line.id != exact_line.id)
                                other_lines.with_context(**ctx).unlink()
                                exact_line.with_context(**ctx).write({'quantity': 1, 'price_unit': unit_price, **taxargs})
                        else:
                            if order.amz_instance_id.is_use_percent_tax:
                                taxargs = self.get_amz_refund_unit_price_ept(invoice_lines, amount)
                                unit_price = taxargs.get('price_unit', 0.0)
                            invoice_lines.with_context(**ctx).write(
                                {'quantity': 1, 'price_unit': unit_price, **taxargs})
                    self.process_settlement_draft_invoices(refund_invoice)
            else:
                if refund_exist.state == 'draft':
                    self.process_settlement_draft_invoices(refund_exist)
                refund_invoice = refund_exist
            lines = statement_line_obj.search([('sale_order_id', '=', order.id), ('refund_invoice_id', '=', False),
                                               ('statement_id', '=', bank_statement.id), ('is_refund_line', '=', True)])
            lines = lines and lines.filtered(
                lambda l, refund_invoice=refund_invoice: round(abs(l.amount), 10) == round(refund_invoice.amount_total, 10))
            lines and lines[0].write({'refund_invoice_id': refund_invoice.id})
        return True

    @staticmethod
    def get_amz_refund_unit_price_ept(invoice_line, amount):
        """
        This method is used to return the unit price and tax percent amount.
        """
        if invoice_line.tax_ids and not invoice_line.tax_ids.price_include:
            unit_price_ept = abs(amount[0])
        else:
            unit_price_ept = abs(amount[0]) + abs(amount[1])
        tax = abs(amount[1])
        item_tax_percent = (tax * 100) / unit_price_ept if unit_price_ept > 0 else 0.00
        return {'line_tax_amount_percent': item_tax_percent, 'price_unit': unit_price_ept}

    def view_bank_statement(self):
        """
        @author: Dimpal added on 10/oct/2019
        This function is used to show generated bank statement from process of settlement report
        Migration done by twinkalc on 28 sep, 2020,
        """
        self.ensure_one()
        action = self.env.ref('account.action_bank_statement_tree', False)
        form_view = self.env.ref('account.view_bank_statement_form', False)
        result = action.sudo().read()[0] if action else {}
        result['views'] = [(form_view and form_view.id or False, 'form')]
        result['res_id'] = self.statement_id.id if self.statement_id else False
        return result

    def auto_import_settlement_report(self, args={}):
        """
        Migration done by twinkalc on 28 sep, 2020,
        This method will auto create settlement record.
        """
        seller_id = args.get('seller_id', False)
        if seller_id:
            seller = self.env[AMAZON_SELLER_EPT].browse(seller_id)
            if seller:
                if seller.settlement_report_last_sync_on:
                    start_date = seller.settlement_report_last_sync_on
                    start_date = datetime.strftime(start_date, DATE_YMDHMS)
                    start_date = datetime.strptime(str(start_date), DATE_YMDHMS)
                else:
                    today = datetime.now()
                    earlier = today - timedelta(days=30)
                    start_date = earlier.strftime(DATE_YMDHMS)
                date_end = datetime.now()
                date_end = date_end.strftime(DATE_YMDHMS)

                vals = {'report_type': 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2',
                        'name': 'Amazon Settlement Reports',
                        'model_obj': self.env['settlement.report.ept'],
                        'sequence': self.env.ref('amazon_ept.seq_import_settlement_report_job'),
                        'tree_id': self.env.ref('amazon_ept.amazon_settlement_report_tree_view_ept'),
                        'form_id': self.env.ref('amazon_ept.amazon_settlement_report_form_view_ept'),
                        'res_model': 'settlement.report.ept',
                        'start_date': start_date,
                        'end_date': date_end
                        }
                report_wiz_rec = self.env['amazon.process.import.export'].create({
                    'seller_id': seller_id,
                })
                report_wiz_rec.get_reports(vals)
        return True

    def process_amazon_settlement_report(self, report):
        """
        This method will import, reconcile and process settlement report.
        """
        if report.state == 'imported':
            report.with_context(is_auto_process=True).reconcile_remaining_transactions()
        else:
            if not report.attachment_id:
                report.with_context(is_auto_process=True).get_report()
            if report.instance_id and report.attachment_id:
                report.with_context(is_auto_process=True).process_settlement_report_file()
                self._cr.commit()
                report.with_context(is_auto_process=True).reconcile_remaining_transactions()
        return True

    def auto_process_settlement_report(self, args={}):
        """
        Migration done by twinkalc on 28 sep, 2020,
        This method will auto process settlement record.
        """
        seller_id = args.get('seller_id', False)
        if seller_id:
            seller = self.env[AMAZON_SELLER_EPT].search([('id', '=', seller_id)])
            if seller:
                settlement_reports = self.search([('seller_id', '=', seller.id),
                                                  ('state', 'in', ['_DONE_', 'imported', 'DONE']),
                                                  ('report_document_id', '!=', False)])
                for report in settlement_reports:
                    self.process_amazon_settlement_report(report)
        return True

    def configure_statement_missing_fees(self):
        """
        Migration done by twinkalc on 28 sep, 2020,
        used to configure the missing fees
        :return: the configure settlement report fees wizard
        """
        view = self.env.ref('amazon_ept.view_configure_settlement_report_fees_ept')
        context = dict(self._context)
        context.update({'settlement_id': self.id, 'seller_id': self.seller_id.id})
        return {
            'name': _('Settlement Report Missing Configure Fees'),
            'type': 'ir.actions.act_window',
            'view_mode': 'form',
            'res_model': 'settlement.report.configure.fees.ept',
            'views': [(view.id, 'form')],
            'view_id': view.id,
            'target': 'new',
            'context': context
        }
