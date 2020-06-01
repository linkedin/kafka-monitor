package com.linkedin.kmf.services;

public interface ServiceFactory {

  Service create() throws Exception;

}
