from aiogram.fsm.state import State, StatesGroup


class VerificationStates(StatesGroup):
    in_progress = State()
