from aiogram.dispatcher.filters.state import State, StatesGroup

class TelethonStates(StatesGroup):
    hash_id_api = State()

class TelethonSessionStates(StatesGroup):
    phone_number = State()
    code_number = State()