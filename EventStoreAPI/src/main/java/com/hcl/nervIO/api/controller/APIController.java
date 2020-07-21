package com.hcl.nervIO.api.controller;

import com.hcl.nervIO.api.service.APIService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static java.text.MessageFormat.format;

import java.util.List;

@CrossOrigin(origins = { "*" }, allowedHeaders = {"*"})
@RestController
@RequestMapping(value = "${application.context}", produces = "application/json")
@Api(value = "${application.context}")
public class APIController {

    public static final Logger logger = LoggerFactory.getLogger(APIController.class);

    @Autowired
    APIService apiService;

    @ApiOperation(value = "getById", notes = "get data by key", response = Object.class)
    @ApiResponses({
            @ApiResponse(code = 200, message = "Loaded last 3 Days Request Details", response = Object.class),
            @ApiResponse(code = 400, message = "Error loading details", response = Exception.class) })
    @RequestMapping(value = "/{productId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<Object> getById(@ApiParam(value = "Product Id") @PathVariable("productId") String id) {
        logger.debug(format("Controller: Search by key from local store for key {0}", id));
        Object object = apiService.getById(id);
		return new ResponseEntity<>(object, HttpStatus.OK);
    }
	
	@ApiOperation(value = "getAll", notes = "get data by key", response = Object.class)
    @ApiResponses({
            @ApiResponse(code = 200, message = "Loaded last 3 Days Request Details", response = Object.class),
            @ApiResponse(code = 400, message = "Error loading details", response = Exception.class) })
    @RequestMapping(value = "/all", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<List> getAll() {
        logger.debug("Controller: Get all records from local store");
        List object = apiService.getAll();
		return new ResponseEntity<>(object, HttpStatus.OK);
    }
}
