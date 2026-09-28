from pydantic import BaseModel, ConfigDict


class PropTrendCoreModel(BaseModel):
    # 서비스가 ORM 행을 model_validate로 바로 넘기므로 속성에서 값을 읽는다.
    model_config = ConfigDict(from_attributes=True)
