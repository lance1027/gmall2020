package com.example.gmall.dw.dwlogger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lance
 * @create 2020-11-03 17:42
 */
//@Controller
    @RestController
public class Demo1Controller {
    //@ResponseBody
    @RequestMapping("testDemo")
    public String testDemo(){
        return "hello demo";
    }
}
