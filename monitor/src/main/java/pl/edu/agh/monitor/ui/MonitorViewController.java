package pl.edu.agh.monitor.ui;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/monitor")
public class MonitorViewController {

    @GetMapping
    String monitorView() {
        return "graph";
    }

}
