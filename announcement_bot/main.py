import argparse
from datetime import datetime as dt

from lib.utils import (
    Announcement,
    ChangePermissionTicker,
    DeleteTicket,
    EditTicket,
    Tools,
    init_args,
)
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update, request
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

CATEGORY, LANGUAGE, LABELS, CONTENT = range(4)
ANNC_ID, NEW_CONTENT = range(2)
CHANGE_PERMISSION, CHOOSE_OPERATION, CHOOSE_PERMISSION = range(3)


class AnnouncementBot:
    INFO_BOT_KEY = "INFO_BOT_KEY"
    BOT_KEY = "MAIN_BOT_KEY"
    TEST_BOT_KEY = "TEST_MAIN_BOT_KEY"
    CONFIRMATION_GROUP = "APPROVE_GROUP_ID"
    REQUEST = request.HTTPXRequest(connection_pool_size=50000, connect_timeout=300, read_timeout=300)

    def __init__(self, is_test: bool) -> None:
        self.is_test = is_test
        self.tools = Tools()
        self.logger = self.tools.get_logger("MainBot")
        self.bot = Bot(self.tools.config[self.BOT_KEY], request=self.REQUEST)
        self.info_bot = Bot(self.tools.config[self.INFO_BOT_KEY], request=self.REQUEST)

    async def post(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user

        if not self.tools.in_whitelist(operator.id):
            await update.message.reply_text(f"Hi {operator.full_name}, You are not in the whitelist")
            return ConversationHandler.END
        self.tools.update_chat_info("download")

        # Create category button, two choice per row
        category = self.tools.get_category()
        name_callback = [(self.tools.get_columns_name(i, "cl"), i) for i in category]
        name_callback.append(("Others", "others"))

        keyboard = []
        for i in range(len(name_callback)):
            if i % 2 == 0:
                keyboard.append([InlineKeyboardButton(name_callback[i][0], callback_data=name_callback[i][1])])
            else:
                keyboard[-1].append(InlineKeyboardButton(name_callback[i][0], callback_data=name_callback[i][1]))
        reply_markup = InlineKeyboardMarkup(keyboard)

        message = f"Hello {operator.full_name}! Please choose a category for your post."

        await update.message.reply_text(message, reply_markup=reply_markup)

        inputs = {
            "id": self.tools.get_simple_id(self.is_test),
            "operation": "post",
            "create_time": dt.now(),
            "creator": operator.full_name,
            "creator_id": operator.id,
        }
        context.user_data["announcement"] = Announcement(**inputs)
        return CATEGORY

    async def choose_category(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query
        context.user_data["announcement"].category = query.data
        if query.data != "others":
            keyboard = [
                [InlineKeyboardButton("English", callback_data="english")],
                [InlineKeyboardButton("Chinese", callback_data="chinese")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = (
                f"You have chosen \n"
                f"**Category** : `{self.tools.get_columns_name(query.data, 'cl')}`\n"
                f"Please choose a language for your post"
            )
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode="MarkdownV2")
            return LANGUAGE
        else:
            message = (
                f"You have chosen \n"
                f"**Category** : `{self.tools.get_columns_name(query.data, 'cl')}`\n"
                f"Please enter the labels or names of the chats you want to post, one per line\n"
            )
            await query.message.edit_text(message, parse_mode="MarkdownV2")
            return LABELS

    async def choose_labels(self, update: Update, context: ContextTypes) -> int:
        text = update.message.text
        labels_or_names = [i.strip() for i in text.split("\n") if i.strip() != ""]
        # read all labels and chat name, if any input in label then append to labels, otherwise append to names
        labels = []
        names = []
        existing_labels = self.tools.get_labels()
        existing_names = self.tools.get_names()
        for i in labels_or_names:
            if i in existing_labels:
                labels.append(i)
            elif i in existing_names:
                names.append(i)
            else:
                if i in ["/cancel", "/cancel@WOO_Announcement_Request_Bot"]:
                    await self.cancel(update, context)
                    return ConversationHandler.END
                await update.message.reply_text(
                    f"Label or name `{i}` not found, please check again", parse_mode="MarkdownV2"
                )
                return LABELS

        context.user_data["announcement"].labels = labels
        context.user_data["announcement"].chats = names

        annc = context.user_data["announcement"]
        message = (
            f"You have chosen \n"
            f"**Category** : `{self.tools.get_columns_name(annc.category, 'cl')}`\n"
            f"**Labels** : `{', '.join(annc.labels)}`\n"
            f"**Chats** : `{', '.join(annc.chats)}`\n"
            f"Please enter the content of your post, in text, photo or video format\n"
        )
        await update.message.reply_text(message, parse_mode="MarkdownV2")
        return CONTENT

    async def choose_language(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query
        context.user_data["announcement"].language = query.data

        annc = context.user_data["announcement"]
        message = (
            f"You have chosen \n"
            f"**Category** : `{self.tools.get_columns_name(annc.category, 'cl')}`\n"
            f"**Language** : `{self.tools.get_columns_name(annc.language, 'al')}`\n"
            f"Please enter the content of your post, in text, photo or video format\n"
        )
        await query.message.edit_text(message, parse_mode="MarkdownV2")
        return CONTENT

    async def choose_content(self, update: Update, context: ContextTypes) -> ConversationHandler.END:
        message = update.message
        operator = update.message.from_user

        # check content type
        photo = message.photo
        video = message.video
        document = message.document
        content_text = message.caption if message.caption else message.text if message.text else ""
        content_html = message.caption_html if message.caption_html else message.text_html if message.text_html else ""

        if len(photo) != 0:  # photo condition
            file_id = photo[3].file_id
            annc_type = "photo"

        elif video is not None:  # video condition
            file_id = video.file_id
            annc_type = "video"

        elif document is not None:
            file_id = document.file_id
            annc_type = "document"

        else:  # Only text condition
            file_id = ""
            annc_type = "text"

        file = await self.tools.save_file(file_id, self.bot)

        update_ = {
            "content_type": annc_type,
            "content_text": content_text,
            "content_html": content_html,
            "file_path": file["path"],
            "available_chats": self.tools.get_chat_by_announcement(context.user_data["announcement"]),
            "status": "pending",
        }

        context.user_data["announcement"].update(**update_)

        method_map = {
            "photo": context.bot.send_photo,
            "video": context.bot.send_video,
            "document": context.bot.send_document,
            "text": context.bot.send_message,
        }

        keyboard = [
            [InlineKeyboardButton("Approve", callback_data=f"approve_{context.user_data['announcement'].id}")],
            [InlineKeyboardButton("Reject", callback_data=f"reject_{context.user_data['announcement'].id}")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # declare variable for confirm message
        chat_id = self.tools.config[self.CONFIRMATION_GROUP] if not self.is_test else "5327851721"
        annc: Announcement = context.user_data["announcement"]

        # send out first part of confirm message, include confirm button
        inputs = {
            "chat_id": chat_id,
            "text": self.tools.get_post_confirm_message(annc),
            "parse_mode": "HTML",
            "reply_markup": reply_markup,
        }
        await context.bot.send_message(**inputs)

        # send out second part of confirm message, to check the announcement content
        if annc_type in ["photo", "video", "document"]:
            inputs = {
                "chat_id": chat_id,
                annc_type: file["id"],
                "caption": annc.content_html,
                "parse_mode": "HTML",
            }

        else:
            inputs = {
                "chat_id": chat_id,
                "text": annc.content_html,
                "parse_mode": "HTML",
            }

        await method_map[annc_type](**inputs)
        self.logger.info(
            f"Announcement {context.user_data['announcement'].id} ticket sent by {operator.full_name}({operator.id})"
        )

        self.tools.input_annc_record(context.user_data["announcement"])
        self.tools.update_annc_record()

        message = (
            f"Your post has been sent to the admin group for approval, "
            f"please wait patiently\.\n"
            f"ID: `{context.user_data['announcement'].id}`"
        )
        await update.message.reply_text(message, parse_mode="MarkdownV2")

        return ConversationHandler.END

    async def confirmation(self, update: Update, context: ContextTypes) -> ConversationHandler.END:
        query = update.callback_query

        operation = query.data.split("_")[0]
        id = query.data.split("_")[1]

        approver = update.effective_user
        annc = self.tools.get_annc_by_id(id)

        if self.tools.is_admin(approver.id):
            inputs = {
                "approver": approver.full_name,
                "approver_id": approver.id,
                "approved_time": dt.now(),
            }

            if operation == "approve":
                inputs["status"] = "approved"
                result = await self.tools.post_annc(annc, self.info_bot)
                parsed_result = self.tools.parse_annc_result(result)
                inputs["record"] = parsed_result
            else:
                inputs["status"] = "rejected"

            annc.update(**inputs)

            repost_message = self.tools.get_report_message(annc)

            await query.message.edit_text(repost_message, parse_mode="HTML")

            self.logger.info(f"Announcement {annc.id} was {annc.status} by {approver.full_name}({approver.id})")

            announcement_db = self.tools.init_collection("AnnouncementDB", "Announcement")
            filter_ = {"id": annc.id}
            update_ = {"$set": annc.__dict__}
            announcement_db.update_one(filter_, update_)

            self.tools.update_annc_record()

            return ConversationHandler.END
        else:
            self.logger.warn(f"Unauthorized user {approver.full_name}({approver.id}) tried to post")

    async def edit(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user
        operator_full_name = self.tools.parse_full_name(operator.full_name)

        if not self.tools.in_whitelist(operator.id):
            await update.message.reply_text(f"Hi {operator.full_name}, You are not in the whitelist")
            return ConversationHandler.END

        inputs = {
            "id": self.tools.get_edit_id(self.is_test),
            "operation": "edit",
            "create_time": dt.now(),
            "creator": operator.full_name,
            "creator_id": operator.id,
        }
        context.user_data["edit_ticket"] = EditTicket(**inputs)

        message = (
            f"Hello {operator_full_name}\! Please enter the ID of the announcement you want to edit\. \n"
            f"Can check the ID in [**Announcement History**](https://docs.google.com/spreadsheets/d/1ZWGIQNCvb_6XLiVIguXaWOjLjP90Os2d1ltOwMT4kqs/edit#gid=1035359090)"
        )

        self.tools.update_annc_record()
        await update.message.reply_text(message, parse_mode="MarkdownV2")

        return ANNC_ID

    async def choose_annc_id(self, update: Update, context: ContextTypes) -> int:
        annc_id = update.message.text

        if annc_id in ["/cancel", "/cancel@WOO_Announcement_Request_Bot"]:
            await self.cancel(update, context)
            return ConversationHandler.END

        annc = self.tools.get_annc_by_id(annc_id)

        if annc is None:
            await update.message.reply_text(f"Announcement {annc_id} not found, please check again")
            return ANNC_ID

        if annc.status != "approved":
            await update.message.reply_text(f"Announcement {annc_id} is not approved, please check again")
            return ANNC_ID

        inputs = {
            "original_id": annc.id,
            "available_chats": annc.record,
            "content_type": annc.content_type,
            "original_content_text": annc.content_text,
            "original_content_html": annc.content_html,
        }
        context.user_data["edit_ticket"].update(**inputs)

        message = f"Announcement `{annc_id}` found, please enter the new content of your post, in text, photo or video format\n"

        await update.message.reply_text(message, parse_mode="MarkdownV2")
        return NEW_CONTENT

    async def choose_edit_content(self, update: Update, context: ContextTypes) -> ConversationHandler.END:
        new_content = update.message.text
        new_content_html = update.message.text_html

        inputs = {
            "new_content_text": new_content,
            "new_content_html": new_content_html,
            "status": "pending",
        }
        context.user_data["edit_ticket"].update(**inputs)

        # in approve ticket only show the old text and new text, not the photo or video
        message = self.tools.get_edit_confirm_message(context.user_data["edit_ticket"])

        keyboard = [
            [InlineKeyboardButton("Approve", callback_data=f"edit_approve_{context.user_data['edit_ticket'].id}")],
            [InlineKeyboardButton("Reject", callback_data=f"edit_reject_{context.user_data['edit_ticket'].id}")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await self.bot.send_message(
            chat_id=self.tools.config[self.CONFIRMATION_GROUP] if not self.is_test else "5327851721",
            text=message,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )

        self.tools.input_edit_record(context.user_data["edit_ticket"])
        self.tools.update_edit_record()

        self.logger.info(
            f"Edit ticket {context.user_data['edit_ticket'].id} sent by {update.message.from_user.full_name}({update.message.from_user.id})"
        )

        message = (
            f"Your edit ticket has been sent to the admin group for approval, "
            f"please wait patiently\.\n"
            f"ID: `{context.user_data['edit_ticket'].id}`"
        )
        await update.message.reply_text(message, parse_mode="MarkdownV2")
        return ConversationHandler.END

    async def edit_confirmation(self, update: Update, context: ContextTypes) -> ConversationHandler.END:
        query = update.callback_query
        status = "_".join(query.data.split("_")[:2])
        ticket_id = query.data.split("_")[-1]
        ticket = self.tools.get_edit_ticket_by_id(ticket_id)

        operator = update.effective_user

        if self.tools.is_admin(operator.id):
            inputs = {
                "approver": operator.full_name,
                "approver_id": operator.id,
                "approved_time": dt.now(),
            }
            if status == "edit_approve":
                await self.tools.edit_annc(ticket, self.info_bot)
                inputs["status"] = "approved"
                ticket.update(**inputs)
            else:
                inputs["status"] = "rejected"
                ticket.update(**inputs)

            self.logger.info(f"Edit ticket {ticket.id} was {ticket.status} by {operator.full_name}({operator.id})")

            repost_message = self.tools.get_report_message(ticket)

            await query.message.edit_text(repost_message, parse_mode="HTML")

            announcement_db = self.tools.init_collection("AnnouncementDB", "Announcement")
            filter_ = {"id": ticket.id}
            update_ = {"$set": ticket.__dict__}
            announcement_db.update_one(filter_, update_)
            self.tools.update_edit_record()

            # only update original annc if approved
            if status == "edit_approve":
                annc = self.tools.get_annc_by_id(ticket.original_id)
                inputs = {
                    "content_text": ticket.new_content_text,
                    "content_html": ticket.new_content_html,
                }
                annc.update(**inputs)
                filter_ = {"id": annc.id}
                update_ = {"$set": annc.__dict__}
                announcement_db.update_one(filter_, update_)

            self.tools.update_annc_record()

        else:
            self.logger.warn(f"Unauthorized user {operator.full_name}({operator.id}) tried to approve editing")

    async def delete(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user
        operator_full_name = self.tools.parse_full_name(operator.full_name)

        if not self.tools.in_whitelist(operator.id):
            await update.message.reply_text(f"Hi {operator.full_name}, You are not in the whitelist")
            return ConversationHandler.END
        else:
            inputs = {
                "id": self.tools.get_delete_id(self.is_test),
                "operation": "delete",
                "create_time": dt.now(),
                "creator": operator_full_name,
                "creator_id": operator.id,
            }
            delete_ticket = DeleteTicket(**inputs)
            context.user_data["delete_ticket"] = delete_ticket
            await update.message.reply_text(
                f"Hello {operator_full_name}! Please enter the ID of the announcement you want to delete."
            )
            return ANNC_ID

    async def choose_delete_id(self, update: Update, context: ContextTypes) -> ConversationHandler.END:
        # get the annc by id, and send the confirmation message to approve group.
        annc_id = update.message.text
        delete_ticket: DeleteTicket = context.user_data["delete_ticket"]

        if annc_id in ["/cancel", "/cancel@WOO_Announcement_Request_Bot"]:
            await self.cancel(update, context)
            return ConversationHandler.END

        annc = self.tools.get_annc_by_id(annc_id)

        if annc is None:
            await update.message.reply_text(
                f"Announcement `{annc_id}` not found, please check again", parse_mode="MarkdownV2"
            )
            return ANNC_ID

        if annc.status != "approved":
            await update.message.reply_text(
                f"Announcement `{annc_id}` is not approved, please check again", parse_mode="MarkdownV2"
            )
            return ANNC_ID

        update_ = {
            "original_id": annc.id,
            "content_type": annc.content_type,
            "original_content_text": annc.content_text,
            "original_content_html": annc.content_html,
            "available_chats": annc.record,
            "status": "pending",
        }
        delete_ticket.update(**update_)

        # send the confirmation message to approve group
        keyboard = [
            [InlineKeyboardButton("Approve", callback_data=f"delete_approve_{delete_ticket.id}")],
            [InlineKeyboardButton("Reject", callback_data=f"delete_reject_{delete_ticket.id}")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        message = self.tools.get_delete_confirm_message(delete_ticket)
        await self.bot.send_message(
            chat_id=self.tools.config[self.CONFIRMATION_GROUP] if not self.is_test else "5327851721",
            text=message,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )

        method_map = {
            "photo": self.bot.send_photo,
            "video": self.bot.send_video,
            "text": self.bot.send_message,
        }
        if annc.content_type in ["photo", "video"]:
            inputs = {
                annc.content_type: annc.file_path,
                "caption": annc.content_html,
                "parse_mode": "HTML",
            }
        else:
            inputs = {
                "text": annc.content_html,
                "parse_mode": "HTML",
            }

        await method_map[annc.content_type](
            chat_id=self.tools.config[self.CONFIRMATION_GROUP] if not self.is_test else "5327851721", **inputs
        )

        self.tools.input_delete_record(delete_ticket)
        self.tools.update_delete_record()

        self.logger.info(
            f"Delete ticket {delete_ticket.id} sent by {update.message.from_user.full_name}({update.message.from_user.id})"
        )
        message = (
            f"Your delete ticket has been sent to the admin group for approval, "
            f"please wait patiently\.\n"
            f"ID: `{delete_ticket.id}`"
        )
        await update.message.reply_text(message, parse_mode="MarkdownV2")
        return ConversationHandler.END

    async def delete_confirmation(self, update: Update, context: ContextTypes) -> ConversationHandler.END:
        response = update.callback_query.data
        status = response.split("_")[1]
        delete_id = response.split("_")[2]
        ticket = self.tools.get_delete_ticket_by_id(delete_id)

        operator = update.effective_user
        if self.tools.is_admin(operator.id):
            inputs = {
                "approver": operator.full_name,
                "approver_id": operator.id,
                "approved_time": dt.now(),
            }
            if status == "approve":
                await self.tools.delete_annc(ticket, self.info_bot)
                inputs["status"] = "approved"
                ticket.update(**inputs)
            else:
                inputs["status"] = "rejected"
                ticket.update(**inputs)

            self.logger.info(f"Delete ticket {ticket.id} was {ticket.status} by {operator.full_name}({operator.id})")

            # update ticket in announcement DB
            annc_db = self.tools.init_collection("AnnouncementDB", "Announcement")
            filter_ = {"id": ticket.id}
            update_ = {"$set": ticket.__dict__}
            annc_db.update_one(filter_, update_, upsert=True)
            self.tools.update_delete_record()

            # edit confirmation message
            repost_message = self.tools.get_report_message(ticket)
            await update.callback_query.message.edit_text(repost_message, parse_mode="HTML")

        else:
            self.logger.warn(f"Unauthorized user {operator.full_name}({operator.id}) tried to delete")

    async def start_change_permission(self, update: Update, context: ContextTypes) -> int:
        """
        Change permission of individual user
        - Add admin and whitelist user process :
            1. use /change_permission command to start the process
            2. select `add` button from the keyboard (`add` or `remove` button)
            3. select `admin` or `whitelist` or `both` button from the keyboard
            4. share a message sent by the user to add as admin or whitelist user
            5. ask current admin users to approve the request
        """

        operator = update.message.from_user
        operator_full_name = self.tools.parse_full_name(operator.full_name)

        if not self.tools.is_admin(operator.id):
            await update.message.reply_text(f"Hi {operator.full_name}, You don't have permission to change permission.")
            return ConversationHandler.END

        inputs = {
            "id": self.tools.get_simple_id(self.is_test),
            "create_time": dt.now(),
            "creator": operator_full_name,
            "creator_id": operator.id,
        }

        ticker = ChangePermissionTicker(**inputs)

        context.user_data["ticket"] = ticker

        callback = [
            [InlineKeyboardButton("Add", callback_data="add")],
            [InlineKeyboardButton("Remove", callback_data="remove")],
            [InlineKeyboardButton("Cancel", callback_data="cancel")],
        ]

        reply_markup = InlineKeyboardMarkup(callback)
        message = f"Hello {operator_full_name}! Please choose the operation you want to do."
        await update.message.reply_text(message, reply_markup=reply_markup)
        return CHANGE_PERMISSION

    async def choose_change_permission_operation(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query
        context.user_data["ticket"].operation = query.data
        callback = [
            InlineKeyboardButton("Admin", callback_data="admin"),
            InlineKeyboardButton("Whitelist", callback_data="whitelist"),
            InlineKeyboardButton("Both", callback_data="admin/whitelist"),
            InlineKeyboardButton("Cancel", callback_data="cancel"),
        ]
        if query.data == "cancel":
            return ConversationHandler.END

        message = f"You have chosen to `{query.data.capitalize()}` permission, please choose the permission type\."
        await query.message.edit_text(message, reply_markup=InlineKeyboardMarkup([callback]), parse_mode="MarkdownV2")
        return CHOOSE_OPERATION

    async def choose_change_permission_type(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query
        ticket = context.user_data["ticket"]

        message = f"Please share the message sent by the user to `{ticket.operation.capitalize()}`  as `{query.data.capitalize()}` user\."

        ticket.operation = f"{ticket.operation}_{query.data}"

        context.user_data["ticket"] = ticket
        await query.message.edit_text(message, parse_mode="MarkdownV2")
        return CHOOSE_PERMISSION

    async def choose_change_permission_user(self, update: Update, context: ContextTypes) -> int:
        ticket: ChangePermissionTicker = context.user_data["ticket"]
        try:
            user = update.message.forward_origin.sender_user
        except AttributeError:
            message = "The user does not open the privacy settings, please ask the user to open the privacy settings then try again\."
            await update.message.reply_text(message, parse_mode="MarkdownV2")
            return CHOOSE_PERMISSION

        update_ = {
            "affected_user": user.full_name,
            "affected_user_id": str(user.id),
            "status": "pending",
        }
        ticket.update(**update_)
        # change permission

        permissions = self.tools.init_collection("AnnouncementDB", "Permissions")

        old_permission = permissions.find_one({"user_id": ticket.affected_user_id})
        values_ = {
            "id": ticket.affected_user_id,
            "name": ticket.affected_user,
            "admin": False if not old_permission else old_permission["admin"],
            "whitelist": False if not old_permission else old_permission["whitelist"],
            "update_time": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        o, ps = ticket.operation.split("_")

        ps_list = ps.split("/")
        for p in ps_list:
            values_[p] = True if o == "add" else False

        permissions.update_one({"id": ticket.affected_user_id}, {"$set": values_}, upsert=True)

        message = f"User {ticket.affected_user} has been `{o.capitalize()}` as `{ps}` user\."
        await update.message.reply_text(message, parse_mode="MarkdownV2")

        self.logger.info(f"User {ticket.affected_user} has been `{o.capitalize()}` as `{ps}` user by {ticket.creator}")
        return ConversationHandler.END

    async def check_permission(self, update: Update, context: ContextTypes) -> ConversationHandler.END:
        operator = update.message.from_user

        if not self.tools.is_admin(operator.id):
            await update.message.reply_text(f"Hi {operator.full_name}, You are not an admin, can't check permission.")
            return ConversationHandler.END

        table = self.tools.get_permission_table()

        message = f"""
        Here is the permission table:\n
        {table}"""
        await update.message.reply_text(message, parse_mode="HTML")

        return ConversationHandler.END

    async def cancel(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user

        message = f"Bye {operator.full_name}! I hope we can talk again some day."

        await update.message.reply_text(message)
        return ConversationHandler.END

    async def help(self, update: Update, context: ContextTypes) -> None:
        operator = update.message.from_user
        operator_full_name = self.tools.parse_full_name(operator.full_name)

        if self.tools.in_whitelist(operator.id):
            help_message = self.tools.get_help_message()

            message = f"Hi {operator_full_name},\n{help_message}"

            await update.message.reply_text(message, parse_mode="MarkdownV2")

    def run(self) -> None:
        self.logger.info("MainBot is running...")
        application = Application.builder().token(self.tools.config[self.BOT_KEY]).build()

        application.add_handler(CommandHandler("help", self.help))
        post_handler = ConversationHandler(
            entry_points=[CommandHandler("post", self.post)],
            states={
                CATEGORY: [CallbackQueryHandler(self.choose_category, pattern=self.tools.get_category_pattern())],
                LANGUAGE: [CallbackQueryHandler(self.choose_language, pattern="^(english|chinese)$")],
                LABELS: [MessageHandler(filters.TEXT, self.choose_labels)],
                CONTENT: [
                    MessageHandler(
                        filters.TEXT & (~filters.COMMAND) | filters.PHOTO | filters.VIDEO | filters.Document.ALL,
                        self.choose_content,
                    )
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            per_chat=False,
        )

        edit_handler = ConversationHandler(
            entry_points=[CommandHandler("edit", self.edit)],
            states={
                ANNC_ID: [MessageHandler(filters.TEXT, self.choose_annc_id)],
                NEW_CONTENT: [MessageHandler(filters.TEXT & (~filters.COMMAND), self.choose_edit_content)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            per_chat=False,
        )

        delete_handler = ConversationHandler(
            entry_points=[CommandHandler("delete", self.delete)],
            states={
                ANNC_ID: [MessageHandler(filters.TEXT, self.choose_delete_id)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            per_chat=False,
        )

        change_permission_handler = ConversationHandler(
            entry_points=[CommandHandler("change_permission", self.start_change_permission)],
            states={
                CHANGE_PERMISSION: [
                    CallbackQueryHandler(self.choose_change_permission_operation, pattern="^(add|remove)$")
                ],
                CHOOSE_OPERATION: [
                    CallbackQueryHandler(
                        self.choose_change_permission_type, pattern="^(admin|whitelist|admin/whitelist)$"
                    )
                ],
                CHOOSE_PERMISSION: [MessageHandler(filters.TEXT, self.choose_change_permission_user)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            per_chat=False,
        )
        application.add_handler(post_handler)
        application.add_handler(edit_handler)
        application.add_handler(delete_handler)
        application.add_handler(change_permission_handler)
        application.add_handler(CallbackQueryHandler(self.confirmation, pattern=r"^(approve|reject)_.*"))
        application.add_handler(CallbackQueryHandler(self.edit_confirmation, pattern=r"^(edit_approve|edit_reject)_.*"))
        application.add_handler(
            CallbackQueryHandler(self.delete_confirmation, pattern=r"^(delete_approve|delete_reject)_.*")
        )
        application.add_handler(CommandHandler("check_permission", self.check_permission))

        application.run_polling()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("InfoBot")
    args = init_args(parser)

    bot = AnnouncementBot(is_test=args.test)
    bot.run()
