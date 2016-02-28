from eda import dto


class EventAdapter(dto.Adapter):
    sender_id = dto.UUID(required=True)
    event_id = dto.UUID(required=True)
    event_type = dto.String(required=True)
