import unittest

from info import InfoBot
from lib.utils import TGTestCases, Tools


class TestInfoBot(unittest.TestCase):
    DB = "AnnouncementDB"
    CHAT_INFO = "ChatInfo"

    def setUp(self):
        self.info_bot = InfoBot(test=True)
        self.tools = Tools()
        self.test_cases = TGTestCases()

    def test_add_or_left_chat(self):
        self.info_bot.chat_status_update(self.test_cases.bot_add(), None)
        self.info_bot.chat_status_update(self.test_cases.bot_left(), None)

    def test_change_chat_title(self):
        self.info_bot.chat_title_update(self.test_cases.chat_rename(), None)

    def test_update_chat_info(self):
        self.info_bot.update_chat_info(self.test_cases.bot_command(command="update_chat_info"), None)

    def test_fill_permission(self):
        self.info_bot.fill_permission(self.test_cases.bot_command(command="fill_permission"), None)

    def test_update_chat_labels(self):
        result = self.info_bot.tools.update_chat_info(direction="down")
        print(result)
        self.info_bot.tools.update_chat_info(direction="up")
        return
