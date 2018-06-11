package pl.edu.agh.sink;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
class ResultIntel {

    private long timestamp;

    private long receivedMessagesCount;

}
