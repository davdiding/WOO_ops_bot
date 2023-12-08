from datetime import datetime, timezone

from telegram import Chat, Message, MessageEntity, Update, User


class TGTestCases:
    def bot_command(self, message: str):
        chat = Chat(first_name="David", id=5327851721, last_name="Ding", type=Chat.PRIVATE, username="Davidding_WG")

        from_user = User(
            first_name="David",
            id=5327851721,
            is_bot=False,
            language_code="en",
            last_name="Ding",
            username="Davidding_WG",
        )

        message_entity = MessageEntity(length=len(message), offset=0, type="bot_command")

        # 創建一個 Message 實例
        message = Message(
            message_id=1533,
            from_user=from_user,
            date=datetime(2023, 12, 4, 7, 29, 7, tzinfo=timezone.utc),
            chat=chat,
            text=message,
            entities=[message_entity],
            group_chat_created=False,
            supergroup_chat_created=False,
            channel_chat_created=False,
            delete_chat_photo=False,
        )
        return Update(update_id=287830049, message=message)
