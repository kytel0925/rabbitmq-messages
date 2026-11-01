<?php

namespace SlothDevGuy\RabbitMQMessages\Exceptions;

use RuntimeException;

/**
 * Thrown when the broker rejects (nacks) or does not confirm a publish.
 */
class MessagePublishException extends RuntimeException {}
