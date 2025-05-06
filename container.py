# core/container.py

from typing import Dict, Any, Type


class DependencyResolutionError(Exception):
    """Raised when a dependency cannot be resolved from the container"""
    pass


class Container:
    """
    Simple dependency injection container that registers and resolves components
    without using fallback values.
    """

    _instance = None
    _services: Dict[Type, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Container, cls).__new__(cls)
            cls._instance._services = {}
        return cls._instance

    def register(self, service_type: Type, implementation: Any) -> None:
        """
        Register a service implementation for a given type

        Args:
            service_type: The type/interface to register
            implementation: The implementation instance
        """
        self._services[service_type] = implementation

    def resolve(self, service_type: Type) -> Any:
        """
        Resolve an implementation for a given type

        Args:
            service_type: The type/interface to resolve

        Returns:
            The registered implementation

        Raises:
            DependencyResolutionError: If no implementation is registered for the type
        """
        if service_type not in self._services:
            raise DependencyResolutionError(f"No implementation registered for {service_type.__name__}")
        return self._services[service_type]


# Create a singleton instance
container = Container()