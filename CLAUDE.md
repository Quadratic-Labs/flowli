# Rules

- Use anchors' skill region metadata to structure the code.
- When exploring code or planing, first use anchors skill to get anchors metadata before Explore/Grep/Read.

When working on flowlet-api (python lib):

- Use google style docstrings.
- Use features from python 3.14+. For example, you can use defered type annotation, classes with generic types, uuid7, ...
- For SQL related things, you SHOULD use SQLAlchemy unified API as much as possible.
- For domain models, you SHOULD use attrs.define(slots=True, kw_only=True).
- For outer-layers like user domain, you SHOULD validate incoming data with pydantic models.

When working on flowlet-web (angular + typescript):
