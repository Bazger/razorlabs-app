package com.razorlabs.persistences.options

import com.razorlabs.config.props.PropsAware


trait PersistenceOptions extends PropsAware {
  var isStreamingRead: Option[Boolean]
}

