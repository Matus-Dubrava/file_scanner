from dataclasses import dataclass


@dataclass
class Message:
    text: str
    is_exact: bool


@dataclass(frozen=True)
class Messages:
    init_success_messages = Message(
        text="Intialized empty .md repository in ", is_exact=False
    )
