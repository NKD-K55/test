<a href="gio-hang" title="Giỏ hàng">
         <span class="cart_header_icon">
          <img src="<?php echo base_url() ?>public/images/cart2.png" alt="Cart">
        </span>
        <span class="box_text">
          <strong class="cart_header_count">Giỏ hàng <span>(<?php  
           if($this->session->has_userdata('Cart')){
            $cart = unserialize($this->session->userdata('Cart'));
            echo count($cart->sanPham);
            
          }else{
            echo 0;
          }
          ?>)</span></strong>
          <span class="cart_price">
            <?php if($this->session->userdata('Cart')):
              $cart=unserialize($this->session->userdata('Cart'));
              ?>
              <?php echo number_format($cart->tongGia).' VNĐ';?>
              <?php else : ?>
                <p>0 VNĐ</p>
              <?php endif; ?>
            </span>
          </span>
        </a>