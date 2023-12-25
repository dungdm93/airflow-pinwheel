import pprint
from typing import Any


class KubernetesObject:
    """
    Attributes:
      openapi_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    openapi_types: dict[str, str] = {}
    attribute_map: dict[str, str] = {}

    def to_dict(self) -> dict[str, Any]:
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in self.openapi_types.items():
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x, value  # type: ignore
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(  # type: ignore
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def to_str(self) -> str:
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self) -> str:
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other: Any) -> bool:
        """Returns true if both objects are equal"""
        if not isinstance(other, KubernetesObject):
            return False

        if hasattr(other, "to_dict") and callable(other.to_dict):
            return self.to_dict() == other.to_dict()

        return False

    def __ne__(self, other: Any) -> bool:
        """Returns true if both objects are not equal"""
        if not isinstance(other, KubernetesObject):
            return True

        if hasattr(other, "to_dict") and callable(other.to_dict):
            return self.to_dict() != other.to_dict()

        return True
