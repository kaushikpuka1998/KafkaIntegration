package com.kgstrivers.kafkaIntegration.Controllers;

import com.kgstrivers.kafkaIntegration.Services.LoadTestService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/test")
public class LoadTestController {

    private final LoadTestService loadTestService;

    public LoadTestController(
            LoadTestService loadTestService) {

        this.loadTestService = loadTestService;
    }

    @PostMapping("/load")
    public String startLoadTest() {

        loadTestService.startLoadTest();

        return "Load test completed";
    }
}
