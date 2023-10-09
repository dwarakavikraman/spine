// Copyright (c) 2019, ElasticRun and contributors
// For license information, please see license.txt

frappe.ui.form.on('Spine Producer Config', {
	refresh: function(frm) {
		frm.add_custom_button(__('Delete Message Log'), function() {
			let filters = null;
			let dialog = new frappe.ui.Dialog({
				title: __("Delete Message Log"),
				fields: [
					{
						fieldtype: "HTML",
						fieldname: "filter_area",
					},
				],
				primary_action_label: __("Delete"),
				primary_action: (values) => {
					console.log(filters);
					frappe.call({
						method: "spine.spine_adapter.doctype.spine_producer_config.spine_producer_config.clear_message_log",
						args: {
							filters: filters,
						},
						callback: function (r) {},
					});
					dialog.hide();
				},
			});

			let filter_group = new frappe.ui.FilterGroup({
				parent: dialog.get_field("filter_area").$wrapper,
				doctype: "Message Log",
				on_change: () => {
					filters = filter_group.get_filters()
				},
			});
			frappe.model.with_doctype("Message Log", () => {});
			dialog.show()
		});
	},
});

frappe.ui.form.on('Spine Producer Handler Mapping', {

	trigger: function(frm, cdt, cdn) {
		if (frm.is_dirty()){
			frappe.msgprint("Please save before first sync");
			return;
		}
		console.log(frm, cdt, cdn, locals[cdt][cdn].document_type)
		let doctype = locals[cdt][cdn].document_type
		if (doctype) {
			let filters = null;
			let dialog = new frappe.ui.Dialog({
				title: __("Trigger"),
				fields: [
					{
						fieldtype: "Select",
						fieldname: "event",
						options: "first_sync\non_update"
					},
					{
						fieldtype: "HTML",
						fieldname: "filter_area",
					},
				],
				primary_action_label: __("Sync"),
				primary_action: (values) => {
					console.log(filters);
					frappe.call({
						method: "spine.spine_adapter.doctype.spine_producer_config.spine_producer_config.trigger_event",
						args: {
							doctype: doctype,
							event: values.event,
							filters: filters,
						},
						callback: function (r) {},
					});
					dialog.hide();
				},
			});

			let filter_group = new frappe.ui.FilterGroup({
				parent: dialog.get_field("filter_area").$wrapper,
				doctype: doctype,
				on_change: () => {
					filters = filter_group.get_filters()
				},
			});
			frappe.model.with_doctype(doctype, () => {
				
			});
			dialog.show()
		}
	}
});
