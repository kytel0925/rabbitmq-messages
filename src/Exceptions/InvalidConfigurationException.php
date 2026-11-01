<?php

namespace SlothDevGuy\RabbitMQMessages\Exceptions;

use InvalidArgumentException;

/**
 * Thrown when config/rabbitmq-messages.php is missing or inconsistent.
 */
class InvalidConfigurationException extends InvalidArgumentException {}
