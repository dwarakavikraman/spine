# -*- coding: utf-8 -*-
# Copyright (c) 2019, ElasticRun and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
import json
from frappe.model.document import Document
from spine.spine_adapter.scheduler.message_processor import publish_message_to_spine, process_message_from_spine
from frappe.query_builder import Interval
from frappe.query_builder.functions import Now

class MessageLog(Document):
	def before_insert(self):
		self.status = 'Pending'

		if self.direction == 'Received':
			self.process_received()

	def process_received(self):
		self.received_at = frappe.utils.now_datetime()
		header = json.loads(self.json_message).get('Header')
		self.event = header.get('Event')
		self.updated_doctype = header.get('DocType')

	def after_insert(self):
		self.process()
		
	def process(self):
		process_bulk = False
		if self.direction == "Sent":
			process_bulk = frappe.get_single("Spine Producer Config").bulk_process
		elif self.direction == "Received":
			process_bulk = frappe.get_single("Spine Consumer Config").bulk_process
		if not process_bulk:
			frappe.utils.background_jobs.enqueue(
				process,
				enqueue_after_commit=True,
				msg_name=self.name,
			)

	@staticmethod
	def clear_old_logs(days=60):
		table = frappe.qb.DocType("Message Log")
		frappe.db.delete(table, filters=(table.modified < (Now() - Interval(days=days))))

def process(msg_name):
	status = frappe.db.sql('select status from `tabMessage Log` where name = %s for update', (msg_name,))
	if (status and status[0][0]) != 'Pending':
		return
	msg = frappe.get_doc('Message Log', msg_name)
	msg.status = 'Processing'
	msg.save(ignore_permissions=True)
	frappe.db.commit()
	if msg.direction == 'Sent':
		publish_message_to_spine(msg, bulk=False)
	elif msg.direction == 'Received':
		process_message_from_spine(msg)

def on_doctype_update():
	frappe.db.add_index('Message Log', ['direction', 'updated_doctype'])